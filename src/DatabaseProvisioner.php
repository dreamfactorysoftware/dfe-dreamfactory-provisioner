<?php namespace DreamFactory\Enterprise\Provisioners\DreamFactory;

use DreamFactory\Enterprise\Common\Contracts\PortableData;
use DreamFactory\Enterprise\Common\Provisioners\BaseDatabaseProvisioner;
use DreamFactory\Enterprise\Common\Provisioners\PortableServiceRequest;
use DreamFactory\Enterprise\Common\Traits\EntityLookup;
use DreamFactory\Enterprise\Database\Exceptions\DatabaseException;
use DreamFactory\Enterprise\Database\Models\Instance;
use DreamFactory\Enterprise\Services\Exceptions\ProvisioningException;
use DreamFactory\Enterprise\Services\Exceptions\SchemaExistsException;
use DreamFactory\Enterprise\Services\Provisioners\ProvisionServiceRequest;
use DreamFactory\Library\Utility\Disk;
use Illuminate\Database\Connection;
use Illuminate\Database\Eloquent\ModelNotFoundException;
use Illuminate\Support\Facades\DB;

class DatabaseProvisioner extends BaseDatabaseProvisioner implements PortableData
{
    //******************************************************************************
    //* Traits
    //******************************************************************************

    use EntityLookup;

    //******************************************************************************
    //* Methods
    //******************************************************************************

    /**
     * @param ProvisionServiceRequest $request
     *
     * @return array|bool
     * @throws \DreamFactory\Enterprise\Services\Exceptions\ProvisioningException
     * @throws \DreamFactory\Enterprise\Services\Exceptions\SchemaExistsException
     */
    protected function doProvision($request)
    {
        $_instance = $request->getInstance();

        $this->info('[provisioning:database] instance "' . $_instance->instance_id_text . '" begin');

        $_serverId = $_instance->db_server_id;

        if (empty($_serverId)) {
            throw new \InvalidArgumentException('Please assign the instance to a database server before provisioning database resources.');
        }

        //  Get a connection to the instance's database server
        list($_db, $_rootConfig, $_rootServer) = $this->getRootDatabaseConnection($_instance);

        //  1. Create a random user and password for the instance
        $_creds = $this->generateSchemaCredentials($_instance);

        $this->debug('[provisioning:database] instance database "' . $_creds['database'] . '" assigned');

        try {
            //	1. Create database
            if (false === $this->createDatabase($_db, $_creds)) {
                try {
                    $this->deprovision($request);
                } catch (\Exception $_ex) {
                    $this->error('[provisioning:database] unable to eradicate Klingons from planet "' . $_creds['database'] . '" after provisioning failure.');
                }

                return false;
            }

            //	2. Grant privileges
            $_result = $this->grantPrivileges($_db, $_creds, $_instance->webServer->host_text);

            if (false === $_result) {
                try {
                    //	Try and get rid of the database we created
                    $this->dropDatabase($_db, $_creds['database']);
                } catch (\Exception $_ex) {
                    //  Ignored, what can we do?
                }

                $this->error('[provisioning:database] instance "' . $_instance->instance_id_text . '" FAILURE');

                return false;
            }
        } catch (ProvisioningException $_ex) {
            throw $_ex;
        } catch (\Exception $_ex) {
            throw new ProvisioningException($_ex->getMessage(), $_ex->getCode());
        }

        //  Fire off a "database.provisioned" event...
        \Event::fire('dfe.database.provisioned', [$this, $request]);

        $this->info('[provisioning:database] instance "' . $_instance->instance_id_text . '" complete');

        return array_merge($_rootConfig, $_creds);
    }

    /**
     * @param ProvisionServiceRequest $request
     * @param array                   $options
     *
     * @return bool
     */
    protected function doDeprovision($request, $options = [])
    {
        $_instance = $request->getInstance();

        $this->info('[deprovisioning:database] instance "' . $_instance->instance_id_text . '" begin');

        //  Get a connection to the instance's database server
        list($_db, $_rootConfig, $_rootServer) = $this->getRootDatabaseConnection($_instance);

        try {
            //	Try and get rid of the database we created
            if (!$this->dropDatabase($_db, $_instance->db_name_text)) {
                throw new ProvisioningException('Unable to delete database "' . $_instance->db_name_text . '".');
            }
        } catch (\Exception $_ex) {
            $this->error('[deprovisioning:database] database "' . $_instance->db_name_text . '" FAILURE: ' . $_ex->getMessage());

            return false;
        }

        //  Fire off a "database.deprovisioned" event...
        \Event::fire('dfe.database.deprovisioned', [$this, $request]);

        $this->info('[deprovisioning:database] instance "' . $_instance->instance_id_text . '" complete');

        return true;
    }

    /** @inheritdoc */
    public function import($request)
    {
        /** @type \ZipArchive $_archive */
        $_archive = null;
        $_from = null;
        $_instance = $request->getInstance();

        $this->info('[provisioning:database:import] instance "' . $_instance->instance_id_text . '" begin');

        //  Grab the target (zip archive) and pull out the target of the import
        $_zip = $request->getTarget();

        /** @noinspection PhpUndefinedMethodInspection */
        $_archive = $_zip->getAdapter()->getArchive();

        foreach ($_zip->listContents() as $_file) {
            if ('dir' != $_file['type'] && false !== strpos($_file['path'], '.database.sql')) {
                $_path = Disk::segment([sys_get_temp_dir(), 'dfe', 'import', sha1($_file['path'])], true);

                if (!$_archive->extractTo($_path, $_file['path'])) {
                    throw new \RuntimeException('Unable to unzip archive file "' . $_file['path'] . '" from snapshot.');
                }

                $_from = Disk::path([$_path, $_file['path']], false);

                if (!$_from || !file_exists($_from)) {
                    throw new \InvalidArgumentException('$from file "' . $_file['path'] . '" missing or unreadable.');
                }

                break;
            }
        }

        /** @type Connection $_db */
        list($_db, $_rootConfig, $_rootServer) = $this->getRootDatabaseConnection($_instance);

        $this->dropDatabase($_db, $_instance->db_name_text);
        $this->createDatabase($_db, ['database' => $_instance->db_name_text]);

        $_results = $this->loadSqlDump($_instance, $_from, $_rootConfig, $request);

        //  Clean up temp space...
        unlink($_from);

        //  Fire off a "database.imported" event...
        \Event::fire('dfe.database.imported', [$this, $request]);

        $this->info('[provisioning:database:import] instance "' . $_instance->instance_id_text . '" complete');

        return $_results;
    }

    /** @inheritdoc */
    public function export($request)
    {
        $_instance = $request->getInstance();
        $this->info('[provisioning:database:export] instance "' . $_instance->instance_id_text . '" begin');

        $_tag = date('YmdHis') . '.' . $_instance->instance_id_text;
        $_workPath = $this->getWorkPath($_tag, true);
        $_target = $_tag . '.database.sql';

        $_command = str_replace(PHP_EOL, null, `which mysqldump`);
        $_template = $_command . ' --compress --delayed-insert {options} >' . ($_workPath . DIRECTORY_SEPARATOR . $_target);
        $_port = $_instance->db_port_nbr;
        $_name = $_instance->db_name_text;

        $_options = [
            '--host=' . escapeshellarg($_instance->db_host_text),
            '--user=' . escapeshellarg($_instance->db_user_text),
            '--password=' . escapeshellarg($_instance->db_password_text),
            '--databases ' . escapeshellarg($_name),
        ];

        if (!empty($_port)) {
            $_options[] = '--port=' . $_port;
        }

        $_command = str_replace('{options}', implode(' ', $_options), $_template);
        exec($_command, $_output, $_return);

        if (0 != $_return) {
            $this->error('[provisioning:database:export] error dumping instance database "' . $_instance->instance_id_text . '".');

            return false;
        }

        //  Copy it over to the snapshot area
        $this->writeStream($_instance->getSnapshotMount(), $_workPath . DIRECTORY_SEPARATOR . $_target, $_target);
        $this->deleteWorkPath($_tag);

        //  Fire off a "database.exported" event...
        \Event::fire('dfe.database.exported', [$this, $request]);

        $this->info('[provisioning:database:export] instance "' . $_instance->instance_id_text . '" complete');

        //  The name of the file in the snapshot mount
        return $_target;
    }

    /**
     * @param Instance $instance
     *
     * @return Connection
     */
    protected function getRootDatabaseConnection(Instance $instance)
    {
        //  Let's go!
        $_dbServerId = $instance->db_server_id;

        //  And stoopids (sic)
        if (empty($_dbServerId)) {
            throw new \RuntimeException('Empty server id given during database resource provisioning for instance');
        }

        try {
            $_server = $this->_findServer($_dbServerId);
        } catch (ModelNotFoundException $_ex) {
            throw new \RuntimeException('Database resource "' . $_dbServerId . '" not found.');
        }

        //  Get the REAL server name
        $_dbServer = $_server->server_id_text;

        //  Build the config, favoring database creds
        $_config = config('database.connections.' . config('database.default'), []);
        $_config = array_merge($_config, $_server->config_text, ['db-server-id' => $_dbServerId]);

        //  Sanity Checks
        if (empty($_config)) {
            throw new \LogicException('Configuration invalid for database resource during provisioning.');
        }

        //  Add it to the connection list
        config(['database.connections.' . $_server->server_id_text => $_config]);

        //  Create a connection and return. It's in Joe Pesce's hands now...
        return [\DB::connection($_dbServer), $_config, $_server];
    }

    /**
     * Generates a unique db-name/user/pass for MySQL for an instance
     *
     * @param Instance $instance
     *
     * @return array
     * @throws SchemaExistsException
     */
    protected function generateSchemaCredentials(Instance $instance)
    {
        $_tries = 0;

        $_dbUser = null;
        $_dbName = $this->generateDatabaseName($instance);
        $_seed = $_dbName . env('APP_KEY') . $instance->instance_name_text;

        //  Make sure our user name is unique...
        while (true) {
            $_baseHash = sha1(microtime(true) . $_seed);
            $_dbUser = substr('u' . $_baseHash, 0, 16);

            if (0 == Instance::where('db_user_text', '=', $_dbUser)->count()) {
                $_sql = 'SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = :schema_name';

                //  Make sure the database name is unique as well.
                /** @noinspection PhpUndefinedMethodInspection */
                $_names = DB::select($_sql, [':schema_name' => $_dbName]);

                if (!empty($_names)) {
                    throw new SchemaExistsException('The schema "' . $_dbName . '" already exists.');
                }

                break;
            }

            if (++$_tries > 10) {
                throw new \LogicException('Unable to locate a non-unique database user name after ' . $_tries . ' attempts.');
            }

            //  Quick snoozy and we try again
            usleep(500000);
        }

        $_creds = [
            'database' => $_dbName,
            'username' => $_dbUser,
            'password' => sha1(microtime(true) . $_seed . $_dbUser . microtime(true)),
        ];

        return $_creds;
    }

    /**
     * @param Connection $db
     * @param array      $creds
     *
     * @return bool
     */
    protected function createDatabase($db, array $creds)
    {
        try {
            $_dbName = $creds['database'];

            if (false === $db->statement(<<<MYSQL
CREATE DATABASE IF NOT EXISTS `{$_dbName}`
MYSQL
                )
            ) {
                throw new DatabaseException(json_encode($db->getPdo()->errorInfo()));
            }

            return true;
        } catch (\Exception $_ex) {
            $this->error('[provisioning:database] create database - failure: ' . $_ex->getMessage());

            return false;
        }
    }

    /**
     * @param Connection $db
     * @param string     $databaseToDrop
     *
     * @return bool
     *
     */
    protected function dropDatabase($db, $databaseToDrop)
    {
        try {
            if (empty($databaseToDrop)) {
                return true;
            }

            $this->debug('[deprovisioning:database] dropping database "' . $databaseToDrop . '"');

            return $db->transaction(function() use ($db, $databaseToDrop) {
                $_result = false;

                if ($db->statement('SET FOREIGN_KEY_CHECKS = 0')) {

                    try {
                        $_result = $db->statement('DROP DATABASE `' . $databaseToDrop . '`');
                        $this->debug('[deprovisioning:database] database "' . $databaseToDrop . '" dropped.');
                    } catch (\Exception $_ex) {
                        \Log::notice('Unable to drop database "' . $databaseToDrop . '": ' . $_ex->getMessage());
                    }
                    finally {
                        $db->statement('SET FOREIGN_KEY_CHECKS = 1');
                    }
                }

                return $_result;
            });
        } catch (\Exception $_ex) {
            $_message = $_ex->getMessage();

            //  If the database is already gone, don't cause an error, but note it.
            if (false !== stripos($_message, 'general error: 1008')) {
                $this->info('[deprovisioning:database] drop database - not performed. database does not exist.');

                return true;
            }

            $this->error('[deprovisioning:database] drop database - failure: ' . $_message);

            return false;
        }
    }

    /**
     * @param Connection $db
     * @param array      $creds
     * @param string     $fromServer
     *
     * @return bool
     */
    protected function grantPrivileges($db, $creds, $fromServer)
    {
        return $db->transaction(function() use ($db, $creds, $fromServer) {
            //  Create users
            $_users = $this->getDatabaseUsers($creds, $fromServer);

            try {
                foreach ($_users as $_user) {
                    $db->statement('GRANT ALL PRIVILEGES ON ' . $creds['database'] . '.* TO ' . $_user . ' IDENTIFIED BY \'' . $creds['password'] . '\'');
                }

                //	Grants for instance database
                return true;
            } catch (\Exception $_ex) {
                $this->error('[provisioning:database] issue grants - failure: ' . $_ex->getMessage());

                return false;
            }
        });
    }

    /**
     * @param Connection $db
     * @param array      $creds
     * @param string     $fromServer
     *
     * @return bool
     */
    protected function revokePrivileges($db, $creds, $fromServer)
    {
        return $db->transaction(function() use ($db, $creds, $fromServer) {
            //  Create users
            $_users = $this->getDatabaseUsers($creds, $fromServer);

            try {
                foreach ($_users as $_user) {
                    //	Grants for instance database
                    if (!($_result = $db->statement('REVOKE ALL PRIVILEGES ON ' . $creds['database'] . '.* FROM ' . $_user))) {
                        $this->error('[deprovisioning:database] error revoking privileges from "' . $_user . '"');
                        continue;
                    }

                    $this->debug('[deprovisioning:database] grants revoked - complete');

                    if (!($_result = $db->statement('DROP USER ' . $_user))) {
                        $this->error('[deprovisioning:database] error dropping user "' . $_user . '"');
                    }

                    $_result && $this->debug('[deprovisioning:database] users dropped > ', $_users);
                }

                return true;
            } catch (\Exception $_ex) {
                $this->error('[deprovisioning:database] revoke grants - failure: ' . $_ex->getMessage());

                return false;
            }
        });
    }

    /**
     * Generates a database name for an instance.
     *
     * @param Instance $instance
     *
     * @return string
     */
    protected function generateDatabaseName(Instance $instance)
    {
        return str_replace('-', '_', strtolower($instance->instance_name_text));
    }

    /**
     * Generates a list of users to de/provision
     *
     * @param array  $creds
     * @param string $fromServer
     *
     * @return array
     */
    protected function getDatabaseUsers($creds, $fromServer)
    {
        return [
            '\'' . $creds['username'] . '\'@\'' . $fromServer . '\'',
            '\'' . $creds['username'] . '\'@\'localhost\'',
        ];
    }

    /**
     * @param \DreamFactory\Enterprise\Database\Models\Instance $instance
     * @param string                                            $filename
     * @param array                                             $dbConfig
     * @param PortableServiceRequest|null                       $request
     *
     * @return string
     */
    protected function loadSqlDump(Instance $instance, $filename, $dbConfig, $request = null)
    {
        if (empty($_command = str_replace(PHP_EOL, null, `which mysql`))) {
            return false;
        }

        if ($request && null !== ($_originalId = $request->get('original-instance-id'))) {
            if (null === ($filename = $this->replaceOriginalInstanceId($filename, $instance, $_originalId))) {
                $this->error('[provisioning:database] sql dump not valid');

                return false;
            }
        }

        $this->debug('[provisioning:database] sql dump "' . $filename . '" munged');

        $_template = $_command . ' {:options} < ' . $filename;
        $_port = $instance->db_port_nbr;

        $_options = [
            '--host=' . escapeshellarg(array_get($dbConfig, 'host')),
            '--user=' . escapeshellarg(array_get($dbConfig, 'username')),
            '--password=' . escapeshellarg(array_get($dbConfig, 'password')),
        ];

        if (!empty($_port)) {
            $_options[] = '--port=' . $_port;
        }

        $_command = str_replace('{:options}', implode(' ', $_options), $_template);

        exec($_command, $_output, $_return);

        if (0 != $_return) {
            $this->error('[provisioning:database] error importing database of instance id "' . $instance->instance_id_text . '".',
                ['output' => $_output, 'command' => $_command, 'return' => $_return]);

            return false;
        }

        return $_output;
    }

    /**
     * @param string   $filename
     * @param Instance $instance
     * @param string   $originalId
     *
     * @return string
     */
    protected function replaceOriginalInstanceId($filename, $instance, $originalId)
    {
        try {
            $_newName = tempnam(sys_get_temp_dir(), $instance->instance_id_text . '.');
            $_fd = fopen($filename, 'r');
            $_fdNew = fopen($_newName, 'w');

            while ($_line = fgets($_fd)) {
                //  Skip create statements. Database is already there...
                if ('create database' == strtolower(substr($_line, 0, 15))) {
                    continue;
                }

                //  Replace the use statement...
                if ('use ' == strtolower(substr($_line, 0, 4))) {
                    $_line = 'USE `' . $instance->db_name_text . '`;';
                }

                fputs($_fdNew, $_line);
            }

            fclose($_fdNew);
            fclose($_fd);

            return $_newName;
        } catch (\Exception $_ex) {
            $this->error('[provisioning:database] error while munging sql dump: ' . $_ex->getMessage());

            return null;
        }
    }
}
