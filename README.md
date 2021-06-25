This package provides a higher level abstraction to @azure/storage-file-datalake which provides low level access. The purpose for this package is to provide a library for managing datalake assets in a more simplistic fashion.

For example,
@azure/storage-file-datalake provides a way to list all files (recursively or otherwise) in a filesystem however it will include all deleted items without offering a way to exclude the deleted items from the list (eg via interface ListPathsOptions) or any path property flagging the path as being deleted.

Methods provided:
 - copy a URL to another URL in the datalake whether it be a URL to a directory or file.