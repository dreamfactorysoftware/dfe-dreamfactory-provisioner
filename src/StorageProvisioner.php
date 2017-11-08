<?php namespace DreamFactory\Enterprise\Provisioners\DreamFactory;

use DreamFactory\Enterprise\Common\Contracts\PortableData;
use DreamFactory\Enterprise\Common\Provisioners\BaseStorageProvisioner;
use DreamFactory\Enterprise\Services\Provisioners\ProvisionServiceRequest;
use DreamFactory\Enterprise\Storage\Facades\InstanceStorage;
use DreamFactory\Library\Utility\Disk;
use Event;
use Exception;
use InvalidArgumentException;
use League\Flysystem\Filesystem;
use League\Flysystem\ZipArchive\ZipArchiveAdapter;
use RuntimeException;

/**
 * DreamFactory Enterprise(tm) and Services Platform File System
 *
 * The default functionality (static::$partitioned is set to TRUE) of this resolver is to provide partitioned
 * layout paths for the hosted storage area. The structure generated is as follows:
 *
 * /mount_point                             <----- Mount point/absolute path of storage area (i.e. "/")
 *      /storage                            <----- Root directory of hosted storage (i.e. "/data/storage")
 *          /zone                           <----- The storage zones (ec2.us-east-1a, ec2.us-west-1b, local, etc.)
 *              /[00-ff]                    <----- The first two bytes of hashes within (the partition)
 *                  /owner-hash
 *                      /.private           <----- owner private storage root
 *                      /instance-hash      <----- Instance storage root
 *                          /.private       <----- Instance private storage root
 *
 * Example paths:
 *
 * /data/storage/ec2.us-east-1a/33/33f58e59068f021c975a1cac49c7b6818de9df5831d89677201b9c3bd98ee1ed/.private
 * /data/storage/ec2.us-east-1a/33/33f58e59068f021c975a1cac49c7b6818de9df5831d89677201b9c3bd98ee1ed/bender/applications
 * /data/storage/ec2.us-east-1a/33/33f58e59068f021c975a1cac49c7b6818de9df5831d89677201b9c3bd98ee1ed/bender/plugins
 * /data/storage/ec2.us-east-1a/33/33f58e59068f021c975a1cac49c7b6818de9df5831d89677201b9c3bd98ee1ed/bender/.private
 * /data/storage/ec2.us-east-1a/33/33f58e59068f021c975a1cac49c7b6818de9df5831d89677201b9c3bd98ee1ed/bender/.private/.cache
 * /data/storage/ec2.us-east-1a/33/33f58e59068f021c975a1cac49c7b6818de9df5831d89677201b9c3bd98ee1ed/bender/.private/config
 * /data/storage/ec2.us-east-1a/33/33f58e59068f021c975a1cac49c7b6818de9df5831d89677201b9c3bd98ee1ed/bender/.private/scripts
 * /data/storage/ec2.us-east-1a/33/33f58e59068f021c975a1cac49c7b6818de9df5831d89677201b9c3bd98ee1ed/bender/.private/scripts.user
 */
class StorageProvisioner extends BaseStorageProvisioner implements PortableData
{
    //******************************************************************************
    //* Constants
    //******************************************************************************

    /**
     * @type string My ID!
     */
    const PROVISIONER_ID = 'dreamfactory';

    //******************************************************************************
    //* Methods
    //******************************************************************************

    /**
     * @param ProvisionServiceRequest $request
     *
     * @return \DreamFactory\Enterprise\Common\Provisioners\BaseResponse|void
     * @throws Exception
     */
    protected function doProvision($request)
    {
        //  Wipe existing stuff
        $_instance = $request->getInstance();
        $_filesystem = $request->getStorage();
        $_packages = $request->get('packages', []);
        $_paths = [];

        //******************************************************************************
        //* Directories are all relative to the request's storage file system
        //******************************************************************************

        //  The instance's base storage path
        $_instanceRootPath = trim($_instance->instance_id_text);
        $this->debug('[provisioning:storage] Instance Root: ' . $_instanceRootPath);

        //  The user's and instance's private path
        $_privateName = InstanceStorage::getPrivatePathName();
        $_ownerPrivatePath = $_privateName;
        $_privatePath = Disk::segment([$_instanceRootPath, $_privateName]);
        $_packagePath = Disk::segment([$_privatePath, config('provisioning.package-path-name')]);

        //logger('Paths', ['private' => $_privatePath, 'package' => $_packagePath]);

        //  Make sure everything exists
        try {

            if (false === $_filesystem->has($_privatePath)) {
                $_filesystem->createDir($_privatePath);
            }

            if (false === $_filesystem->has($_ownerPrivatePath)) {
                $_filesystem->createDir($_ownerPrivatePath);
            }

            if (false === $_filesystem->has($_packagePath)) {
                $_filesystem->createDir($_packagePath);
            }

            //  Now collect ancillary sub-directories
            foreach (config('provisioning.public-paths', []) as $_path) {
                $_paths[] = Disk::segment([$_instanceRootPath, $_path]);
            }

            foreach (config('provisioning.private-paths', []) as $_path) {
                $_paths[] = Disk::segment([$_privatePath, $_path]);
            }

            foreach (config('provisioning.owner-private-paths', []) as $_path) {
                $_paths[] = Disk::segment([$_ownerPrivatePath, $_path]);
            }

            //  And create at once
            foreach ($_paths as $_path) {
                !$_filesystem->has($_path) && $_filesystem->createDir($_path);
            }

            $this->debug('[provisioning:storage] structure built',
                array_merge([
                    'private' => $_privatePath,
                    'owner-private' => $_ownerPrivatePath,
                    'package' => $_packagePath
                ], $_paths));

            //  Copy any package files...
            if (!empty($_packages)) {
                foreach ($_packages as $_index => $_package) {
                    try {
                        $_packageName = md5($_package) . '-upload-package.zip';
                        /** @noinspection PhpUndefinedMethodInspection */
                        if (false ===
                            copy($_package, Disk::path([
                                $_filesystem->getAdapter()->getPathPrefix(),
                                $_packagePath,
                                $_packageName
                            ]))) {
                            throw new \Exception();
                        }

                        $_packages[$_index] = $_packageName;
                        $this->debug('[provisioning:storage] * copied package "' . $_package . '" to package-path');
                    } catch (\Exception $_ex) {
                        $this->error('[provisioning:storage] error copying package file to private path',
                            ['message' => $_ex->getMessage(), 'source' => $_package, 'destination' => $_packagePath]);
                    }
                }

                $request->setInstance($_instance->setPackages($_packages));
            } else {
                $this->debug('[dfe.storage-provisioner.do-provision] * no packages to install');
            }
        } catch (Exception $_ex) {
            $this->error('[dfe.storage-provisioner.do-provision] error creating directory structure: ' . $_ex->getMessage());

            return false;
        }

//  Fire off a "storage.provisioned" event...
Event::fire('dfe.storage.provisioned', [$this, $request]);

$this->info('[dfe.storage-provisioner.do-provision] instance "' . $_instance->instance_id_text . '" complete');

$this->setPrivatePath($_privatePath);
$this->setOwnerPrivatePath($_ownerPrivatePath);
$this->setPackagePath($_packagePath);

return true;
}

/**
 * Deprovision an instance
 *
 * @param ProvisionServiceRequest $request
 * @param array                   $options
 *
 * @return bool
 */
protected
function doDeprovision($request, $options = [])
{
    $_instance = $request->getInstance();
    $_filesystem = $request->getStorage();
    $_storagePath = $_instance->instance_id_text;

    $this->info('[dfe.storage-provisioner.do-deprovision] instance "' . $_instance->instance_id_text . '" begin');

    //  I'm not sure how hard this tries to delete the directory
    if (!$_filesystem->has($_storagePath)) {
        $this->notice('[dfe.storage-provisioner.do-deprovision] unable to stat storage path "' .
            $_storagePath .
            '". not deleting!');

        return false;
    }

    if (!$_filesystem->deleteDir($_storagePath)) {
        $this->error('[dfe.storage-provisioner.do-deprovision] error deleting storage area "' . $_storagePath . '"');

        return false;
    }

    //  Fire off a "storage.deprovisioned" event...
    Event::fire('dfe.storage.deprovisioned', [$this, $request]);

    $this->info('[dfe.storage-provisioner.do-deprovision] instance "' . $_instance->instance_id_text . '" complete');

    return true;
}

/** @inheritdoc */
public
function import($request)
{
    $_from = null;
    $_instance = $request->getInstance();
    $_mount = $_instance->getStorageMount();

    $this->info('[dfe.storage-provisioner.import] instance "' . $_instance->instance_id_text . '" begin');

    //  Grab the target (zip archive) and pull out the target of the import
    $_zip = $request->getTarget();
    /** @var \ZipArchive $_archive */
    /** @noinspection PhpUndefinedMethodInspection */
    $_archive = $_zip->getAdapter()->getArchive();

    $_path = null;

    if (!($_mount instanceof Filesystem)) {
        $_mount = new Filesystem(new ZipArchiveAdapter($_mount));
    }

    //  If "clean" == true, storage is wiped clean before restore
    if (true === $request->get('clean', false)) {
        $_mount->deleteDir('./');
    }

    try {
        $_restored = $this->_extractZipContents($_zip, $_archive, $_mount);
    } catch (Exception $_ex) {
        $this->error('[dfe.storage-provisioner._extractZipContents] : ' . $_ex->getMessage());

        return false;
    }

    //  Fire off a "storage.imported" event...
    Event::fire('dfe.storage.imported', [$this, $request]);

    $this->info('[dfe.storage-provisioner.import] instance "' . $_instance->instance_id_text . '" complete');

    return $_restored;
}

protected
function _extractZipContents($_zip, $_archive, $_mount)
{

    $_restored = [];

    foreach ($_zip->listContents() as $_file) {
        if ('dir' != $_file['type'] && false !== strpos($_file['path'], '.storage.zip')) {
            $_from = Disk::segment([sys_get_temp_dir(), 'dfe', 'import', sha1($_file['path'])], true);

            if (!$_archive->extractTo($_from)) {
                throw new RuntimeException('Unable to unzip archive file "' . $_file['path'] . '" from snapshot.');
            }

            $_path = Disk::path([$_from, $_file['path']], false);

            if (!$_path || !file_exists($_path)) {
                throw new InvalidArgumentException('$from file "' . $_file['path'] . '" missing or unreadable.');
            }

            $nestedZip = new Filesystem(new ZipArchiveAdapter($_path));
            /* Handles nested zip functionality */
            if (is_object($nestedZip)) {
                $_nestedArchive = $nestedZip->getAdapter()->getArchive();
                $_nestedArchive->extractTo($_mount->getAdapter()->getPathPrefix());
            }
        }
        $_restored[] = $_file;
    }
    /*Cleanup temporary location */
    $_path && is_dir(dirname($_path)) && Disk::deleteTree(dirname($_path));

    return $_restored;
}

/** @inheritdoc */
public
function export($request)
{
    $_instance = $request->getInstance();
    $_mount = $_instance->getStorageMount();

    $this->info('[dfe.storage-provisioner.export] instance "' . $_instance->instance_id_text . '" begin');

    $_tag = date('YmdHis') . '.' . $_instance->instance_id_text;
    $_workPath = $this->getWorkPath($_tag, true);
    $_target = $_tag . '.storage.zip';

    //  Create our zip container
    if (false !== ($_file = static::archiveTree($_mount, $_workPath . DIRECTORY_SEPARATOR . $_target))) {
        //  Copy it over to the snapshot area
        $this->writeStream($_instance->getSnapshotMount(), $_workPath . DIRECTORY_SEPARATOR . $_target, $_target);
    }

    !$request->get('keep-work', false) && $this->deleteWorkPath($_tag);

    //  Fire off a "storage.exported" event...
    Event::fire('dfe.storage.exported', [$this, $request]);

    $this->info('[dfe.storage-provisioner.export] instance "' . $_instance->instance_id_text . '" complete');

    //  The name of the file in the snapshot mount
    return $_file;
}
}
