using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security.AccessControl;

using DokanNet;
using DokanNet.Logging;

using static DokanNet.FormatProviders;

using FileAccess = DokanNet.FileAccess;

/*
 * Todo: Remove this section if all fixes to remove warnings are acknowledged and tested.
 *
 * 1. Made '_ReadableFileSystem' and '_WriteableFileSystem' properties.
 *    (Previously fields.)
 * 2. Added 'StringComparison.Ordinal' modifier on all culture-sensitive input comparisons.
 * 3. Added 'CultureInfo.InvariantCulture' on all culture-sensitive outputs.
 * 4. Added null-check on parameters that are not used with '?.' syntax.
 *    (Previously they throw 'NullReferenceException'; now 'ArgumentNullException'.)
 */

namespace DokanFS
{
    public class DokanFileSystemAdapter : IDokanOperations
    {
        private const FileAccess DataAccess = FileAccess.ReadData | FileAccess.WriteData | FileAccess.AppendData |
                                              FileAccess.Execute |
                                              FileAccess.GenericExecute | FileAccess.GenericWrite |
                                              FileAccess.GenericRead;

        private const FileAccess DataWriteAccess = FileAccess.WriteData | FileAccess.AppendData |
                                                   FileAccess.Delete |
                                                   FileAccess.GenericWrite;

        private readonly ConsoleLogger logger = new ConsoleLogger("[FS] ");

        protected IReadableFileSystem Readable { get; }
        protected IWriteableFileSystem Writable { get; }

        public DokanFileSystemAdapter(IReadableFileSystem fileSystem)
        {
            Readable = fileSystem;
            Writable = fileSystem as IWriteableFileSystem;
        }

        protected NtStatus Trace(string method, string fileName, IDokanFileInfo info, NtStatus result,
            params object[] parameters)
        {
#if TRACE
            if (result != NtStatus.Success)
            {
                var extraParameters = parameters != null && parameters.Length > 0
                    ? ", " + string.Join(", ", parameters.Select(x => string.Format(DefaultFormatProvider, "{0}", x)))
                    : string.Empty;

                logger.Debug(DokanFormat($"{method}('{fileName}', {info}{extraParameters}) -> {result}"));
            }
#endif
            return result;
        }

        private NtStatus Trace(string method, string fileName, IDokanFileInfo info,
            FileAccess access, FileShare share, FileMode mode, FileOptions options, FileAttributes attributes,
            NtStatus result)
        {
#if TRACE
            if (result != NtStatus.Success)
            {
                logger.Debug(
                    DokanFormat(
                        $"{method}('{fileName}', {info}, [{access}], [{share}], [{mode}], [{options}], [{attributes}]) -> {result}"));
            }
#endif
            return result;
        }

        #region Implementation of IDokanOperations

        public NtStatus CreateFile(string fileName, FileAccess access, FileShare share, FileMode mode,
            FileOptions options, FileAttributes attributes, IDokanFileInfo info)
        {
            if (fileName is null) throw new ArgumentNullException(nameof(fileName));
            if (info is null) throw new ArgumentNullException(nameof(info));

            var result = DokanResult.Success;

            if (fileName.EndsWith("*", StringComparison.Ordinal))
            {
                fileName = fileName.TrimEnd('*', '/', '\\');
                if (string.IsNullOrWhiteSpace(fileName))
                {
                    fileName = "/";
                }
            }
            if (info.IsDirectory)
            {
                try
                {
                    Readable.CheckNodeExists(fileName, out bool isDirectory, out bool isFile);
                    switch (mode)
                    {
                        case FileMode.Open:
                            // Result: NotADirectory, PathNotFound, Exception (Access Denied)
                            if (!isDirectory && !isFile)
                            {
                                return Trace(nameof(CreateFile), fileName, info, access, share, mode, options,
                                    attributes, DokanResult.PathNotFound);
                            }
                            else if (isDirectory && isFile)
                            {
                                return Trace(nameof(CreateFile), fileName, info, access, share, mode, options,
                                    attributes, DokanResult.PathNotFound);
                            }
                            else if (isFile)
                            {
                                return Trace(nameof(CreateFile), fileName, info, access, share, mode, options,
                                    attributes, DokanResult.NotADirectory);
                            }
                            Writable?.IsEmptyDirectory(fileName);
                            // you can't list the directory
                            break;

                        case FileMode.CreateNew:
                            // Result: FileExists, AlreadyExists
                            if (Writable != null)
                            {
                                if (isDirectory || isFile)
                                {
                                    return Trace(nameof(CreateFile), fileName, info, access, share, mode, options,
                                        attributes, DokanResult.AlreadyExists);
                                }
                                Writable.CreateDirectory(fileName);
                            }
                            else
                            {
                                return Trace(nameof(CreateFile), fileName, info, access, share, mode, options,
                                    attributes, DokanResult.AccessDenied);
                            }
                            break;
                    }
                }
                catch (UnauthorizedAccessException)
                {
                    return Trace(nameof(CreateFile), fileName, info, access, share, mode, options, attributes,
                        DokanResult.AccessDenied);
                }
            }
            else
            {
                var pathExists = false;
                var pathIsDirectory = false;

                var readWriteAttributes = (access & DataAccess) == 0;
                var readAccess = (access & DataWriteAccess) == 0;

                try
                {
                    Readable.CheckNodeExists(fileName, out bool isDirectory, out bool isFile);
                    pathExists = (isDirectory || isFile);
                    pathIsDirectory = isDirectory;
                }
                catch // (IOException)
                {
                }

                switch (mode)
                {
                    case FileMode.Open:

                        if (pathExists)
                        {
                            // check if driver only wants to read attributes, security info, or open directory
                            if (readWriteAttributes || pathIsDirectory)
                            {
                                if (pathIsDirectory && (access & FileAccess.Delete) == FileAccess.Delete
                                    && (access & FileAccess.Synchronize) != FileAccess.Synchronize)
                                {
                                    // It is a DeleteFile request on a directory
                                    return Trace(nameof(CreateFile), fileName, info, access, share, mode, options,
                                        attributes, DokanResult.AccessDenied);
                                }

                                info.IsDirectory = pathIsDirectory;
                                info.Context = new object();
                                // must set it to something if you return DokanError.Success

                                return Trace(nameof(CreateFile), fileName, info, access, share, mode, options,
                                    attributes, DokanResult.Success);
                            }
                        }
                        else
                        {
                            Console.WriteLine($"FileMode.Open - {fileName}");
                            return Trace(nameof(CreateFile), fileName, info, access, share, mode, options, attributes,
                                DokanResult.FileNotFound);
                        }
                        break;

                    case FileMode.CreateNew:

                        if (pathExists)
                        {
                            return Trace(nameof(CreateFile), fileName, info, access, share, mode, options, attributes,
                                DokanResult.FileExists);
                        }
                        break;

                    case FileMode.Truncate:

                        if (!pathExists)
                        {
                            return Trace(nameof(CreateFile), fileName, info, access, share, mode, options, attributes,
                                DokanResult.FileNotFound);
                        }
                        break;
                }

                try
                {
                    info.Context = Readable.CreateFileContext(fileName, mode, readAccess, share, options);

                    if (pathExists && (mode == FileMode.OpenOrCreate || mode == FileMode.Create))
                    {
                        result = DokanResult.AlreadyExists;
                    }

                    if (Writable != null)
                    {
                        if (mode == FileMode.CreateNew || mode == FileMode.Create) // Files are always created as Archive
                        {
                            attributes |= FileAttributes.Archive;
                        }

                        try
                        {
                            Writable.SetFileAttributes(fileName, attributes);
                        }
                        catch
                        {
                            // Not Supported
                        }
                    }
                }
                catch (UnauthorizedAccessException) // don't have access rights
                {
                    if (info.Context != null)
                    {
                        // returning AccessDenied cleanup and close won't be called,
                        // so we have to take care of them here
                        Readable.CloseFileContext(info.Context);
                        info.Context = null;
                    }
                    return Trace(nameof(CreateFile), fileName, info, access, share, mode, options, attributes,
                        DokanResult.AccessDenied);
                }
                catch (DirectoryNotFoundException)
                {
                    return Trace(nameof(CreateFile), fileName, info, access, share, mode, options, attributes,
                        DokanResult.PathNotFound);
                }
                catch (Exception ex)
                {
                    var hr = (uint) Marshal.GetHRForException(ex);
                    switch (hr)
                    {
                        case 0x80070020: //Sharing violation
                            return Trace(nameof(CreateFile), fileName, info, access, share, mode, options, attributes,
                                DokanResult.SharingViolation);

                        default:
                            throw;
                    }
                }
            }
            return Trace(nameof(CreateFile), fileName, info, access, share, mode, options, attributes,
                result);
        }

        public void Cleanup(string fileName, IDokanFileInfo info)
        {
            if (info is null) throw new ArgumentNullException(nameof(info));

            if (info.Context != null)
            {
                Readable.CloseFileContext(info.Context);
                info.Context = null;
            }

            if (info.DeleteOnClose && Writable != null)
            {
                if (info.IsDirectory)
                {
                    Writable.DeleteDirectory(fileName);
                }
                else
                {
                    Writable.DeleteFile(fileName);
                }
            }
            Trace(nameof(Cleanup), fileName, info, DokanResult.Success);
        }

        public void CloseFile(string fileName, IDokanFileInfo info)
        {
            if (info is null) throw new ArgumentNullException(nameof(info));

            if (info.Context != null)
            {
                Readable.CloseFileContext(info.Context);
                info.Context = null;
            }

            Trace(nameof(CloseFile), fileName, info, DokanResult.Success);
            // could recreate cleanup code here but this is not called sometimes
        }

        public NtStatus ReadFile(string fileName, byte[] buffer, out int bytesRead, long offset, IDokanFileInfo info)
        {
            if (buffer is null) throw new ArgumentNullException(nameof(buffer));

            // info.Context == null, memory mapped read
            // info.Context != null, the context should be locked to protect from overlapped read
            Readable.ReadFile(fileName, offset, buffer.Length, buffer, out bytesRead);
            return Trace(nameof(ReadFile), fileName, info, DokanResult.Success, "out " + bytesRead.ToString(CultureInfo.InvariantCulture),
                offset.ToString(CultureInfo.InvariantCulture));
        }

        public NtStatus WriteFile(string fileName, byte[] buffer, out int bytesWritten, long offset, IDokanFileInfo info)
        {
            // info.Context != null, the context should be locked to protect from overlapped write
            if (Writable != null)
            {
                Writable.WriteFile(fileName, buffer, out bytesWritten, offset);
                return Trace(nameof(WriteFile), fileName, info, DokanResult.Success, "out " + bytesWritten.ToString(CultureInfo.InvariantCulture),
                    offset.ToString(CultureInfo.InvariantCulture));
            }
            else
            {
                bytesWritten = 0;
                return Trace(nameof(WriteFile), fileName, info, DokanResult.AccessDenied, "out " + bytesWritten.ToString(CultureInfo.InvariantCulture),
                    offset.ToString(CultureInfo.InvariantCulture));
            }
        }

        public NtStatus FlushFileBuffers(string fileName, IDokanFileInfo info)
        {
            if (info is null) throw new ArgumentNullException(nameof(info));

            if (Writable == null)
            {
                return Trace(nameof(FlushFileBuffers), fileName, info, DokanResult.AccessDenied);
            }

            try
            {
                if (info.Context != null)
                {
                    Writable.FlushFileBuffers(info.Context);
                }
                return Trace(nameof(FlushFileBuffers), fileName, info, DokanResult.Success);
            }
            catch (IOException)
            {
                return Trace(nameof(FlushFileBuffers), fileName, info, DokanResult.DiskFull);
            }
        }

        public NtStatus GetFileInformation(string fileName, out FileInformation fileInfo, IDokanFileInfo info)
        {
            if (fileName is null) throw new ArgumentNullException(nameof(fileName));
            if (info is null) throw new ArgumentNullException(nameof(info));

            // may be called with info.Context == null, but usually it isn't
            if (fileName.EndsWith("*", StringComparison.Ordinal))
            {
                fileName = fileName.TrimEnd('*', '/', '\\');
                if (string.IsNullOrWhiteSpace(fileName))
                {
                    fileName = "/";
                }
            }
            info.TryResetTimeout(60000);
            Readable.GetFileInformation(fileName, out fileInfo);
            return Trace(nameof(GetFileInformation), fileName, info, DokanResult.Success);
        }

        public NtStatus FindFiles(string fileName, out IList<FileInformation> files, IDokanFileInfo info)
        {
            // This function is not called because FindFilesWithPattern is implemented
            // Return DokanResult.NotImplemented in FindFilesWithPattern to make FindFiles called
            files = FindFilesHelper(fileName, "*");
            return Trace(nameof(FindFiles), fileName, info, DokanResult.Success);
        }

        public NtStatus SetFileAttributes(string fileName, FileAttributes attributes, IDokanFileInfo info)
        {
            if (Writable == null)
            {
                return Trace(nameof(SetFileAttributes), fileName, info, DokanResult.AccessDenied, attributes.ToString());
            }

            try
            {
                // MS-FSCC 2.6 File Attributes : There is no file attribute with the value 0x00000000
                // because a value of 0x00000000 in the FileAttributes field means that the file attributes for this file MUST NOT be changed when setting basic information for the file
                if (attributes != 0)
                {
                    Writable.SetFileAttributes(fileName, attributes);
                }
                return Trace(nameof(SetFileAttributes), fileName, info, DokanResult.Success, attributes.ToString());
            }
            catch (UnauthorizedAccessException)
            {
                return Trace(nameof(SetFileAttributes), fileName, info, DokanResult.AccessDenied, attributes.ToString());
            }
            catch (FileNotFoundException)
            {
                return Trace(nameof(SetFileAttributes), fileName, info, DokanResult.FileNotFound, attributes.ToString());
            }
            catch (DirectoryNotFoundException)
            {
                return Trace(nameof(SetFileAttributes), fileName, info, DokanResult.PathNotFound, attributes.ToString());
            }
        }

        public NtStatus SetFileTime(string fileName, DateTime? creationTime, DateTime? lastAccessTime,
            DateTime? lastWriteTime, IDokanFileInfo info)
        {
            if (Writable == null)
            {
                return Trace(nameof(SetFileTime), fileName, info, DokanResult.AccessDenied, creationTime, lastAccessTime,
                    lastWriteTime);
            }

            try
            {
                Writable.SetFileTime(fileName, creationTime, lastAccessTime, lastWriteTime);
                return Trace(nameof(SetFileTime), fileName, info, DokanResult.Success, creationTime, lastAccessTime,
                    lastWriteTime);
            }
            catch (UnauthorizedAccessException)
            {
                return Trace(nameof(SetFileTime), fileName, info, DokanResult.AccessDenied, creationTime, lastAccessTime,
                    lastWriteTime);
            }
            catch (FileNotFoundException)
            {
                return Trace(nameof(SetFileTime), fileName, info, DokanResult.FileNotFound, creationTime, lastAccessTime,
                    lastWriteTime);
            }
        }

        public NtStatus DeleteFile(string fileName, IDokanFileInfo info)
        {
            if (Writable == null)
            {
                return Trace(nameof(DeleteFile), fileName, info, DokanResult.AccessDenied);
            }

            Readable.CheckNodeExists(fileName, out bool isDirectory, out bool isFile);
            if (isFile && !isDirectory)
            {
                return Trace(nameof(DeleteFile), fileName, info, DokanResult.Success);
            }
            else if (!isFile && !isDirectory)
            {
                return Trace(nameof(DeleteFile), fileName, info, DokanResult.FileNotFound);
            }
            else
            {
                return Trace(nameof(DeleteFile), fileName, info, DokanResult.AccessDenied);
            }
        }

        public NtStatus DeleteDirectory(string fileName, IDokanFileInfo info)
        {
            if (Writable == null)
            {
                return Trace(nameof(DeleteDirectory), fileName, info, DokanResult.AccessDenied);
            }

            var isEmptyDirectory = Writable.IsEmptyDirectory(fileName);
            return Trace(nameof(DeleteDirectory), fileName, info,
                isEmptyDirectory ? DokanResult.Success : DokanResult.DirectoryNotEmpty);
        }

        public NtStatus MoveFile(string oldName, string newName, bool replace, IDokanFileInfo info)
        {
            if (info is null) throw new ArgumentNullException(nameof(info));

            if (Writable == null)
            {
                return Trace(nameof(MoveFile), oldName, info, DokanResult.AccessDenied);
            }

            if (info.Context != null)
            {
                Readable.CloseFileContext(info.Context);
                info.Context = null;
            }

            Readable.CheckNodeExists(oldName, out bool isDirectory_OldName, out bool isFile_OldName);
            Readable.CheckNodeExists(newName, out bool isDirectory_NewName, out bool isFile_NewName);

            try
            {
                if (!isDirectory_NewName && !isFile_NewName)
                {
                    if (isDirectory_OldName && !isFile_OldName)
                    {
                        Writable.MoveDirectory(oldName, newName);
                        return Trace(nameof(MoveFile), oldName, info, DokanResult.Success, newName,
                            replace.ToString(CultureInfo.InvariantCulture));
                    }
                    else if (!isDirectory_OldName && isFile_OldName)
                    {
                        Writable.MoveFile(oldName, newName);
                        return Trace(nameof(MoveFile), oldName, info, DokanResult.Success, newName,
                            replace.ToString(CultureInfo.InvariantCulture));
                    }
                }
                else if (replace)
                {
                    if (info.IsDirectory) // Cannot replace directory destination - See MOVEFILE_REPLACE_EXISTING
                    {
                        return Trace(nameof(MoveFile), oldName, info, DokanResult.AccessDenied, newName,
                            replace.ToString(CultureInfo.InvariantCulture));
                    }

                    Writable.DeleteFile(newName);
                    Writable.MoveFile(oldName, newName);
                    return Trace(nameof(MoveFile), oldName, info, DokanResult.Success, newName,
                        replace.ToString(CultureInfo.InvariantCulture));
                }
            }
            catch (UnauthorizedAccessException)
            {
                return Trace(nameof(MoveFile), oldName, info, DokanResult.AccessDenied, newName,
                    replace.ToString(CultureInfo.InvariantCulture));
            }
            return Trace(nameof(MoveFile), oldName, info, DokanResult.FileExists, newName,
                replace.ToString(CultureInfo.InvariantCulture));
        }

        public NtStatus SetEndOfFile(string fileName, long length, IDokanFileInfo info)
        {
            if (Writable == null)
            {
                return Trace(nameof(SetEndOfFile), fileName, info, DokanResult.AccessDenied);
            }

            try
            {
                Writable.SetFileLength(fileName, length);
                return Trace(nameof(SetEndOfFile), fileName, info, DokanResult.Success,
                    length.ToString(CultureInfo.InvariantCulture));
            }
            catch (IOException)
            {
                return Trace(nameof(SetEndOfFile), fileName, info, DokanResult.DiskFull,
                    length.ToString(CultureInfo.InvariantCulture));
            }
        }

        public NtStatus SetAllocationSize(string fileName, long length, IDokanFileInfo info)
        {
            // dokan/setfile.c
            // It calls DokanOperations->SetEndOfFile() for SetAllocationSize()

            if (Writable == null)
            {
                return Trace(nameof(SetAllocationSize), fileName, info, DokanResult.AccessDenied);
            }

            try
            {
                Writable.SetFileLength(fileName, length);
                return Trace(nameof(SetAllocationSize), fileName, info, DokanResult.Success,
                    length.ToString(CultureInfo.InvariantCulture));
            }
            catch (IOException)
            {
                return Trace(nameof(SetAllocationSize), fileName, info, DokanResult.DiskFull,
                    length.ToString(CultureInfo.InvariantCulture));
            }
        }

        public NtStatus LockFile(string fileName, long offset, long length, IDokanFileInfo info)
        {
            return DokanResult.NotImplemented;
        }

        public NtStatus UnlockFile(string fileName, long offset, long length, IDokanFileInfo info)
        {
            return DokanResult.NotImplemented;
        }

        public NtStatus GetDiskFreeSpace(out long freeBytesAvailable, out long totalNumberOfBytes, out long totalNumberOfFreeBytes, IDokanFileInfo info)
        {
            try
            {
                Readable.GetDiskFreeSpace(out freeBytesAvailable, out totalNumberOfBytes, out totalNumberOfFreeBytes);
                return Trace(nameof(GetDiskFreeSpace), null, info, DokanResult.Success, "out " + freeBytesAvailable.ToString(CultureInfo.InvariantCulture),
                    "out " + totalNumberOfBytes.ToString(CultureInfo.InvariantCulture), "out " + totalNumberOfFreeBytes.ToString(CultureInfo.InvariantCulture));
            }
            catch
            {
                freeBytesAvailable = 0;
                totalNumberOfBytes = 0;
                totalNumberOfFreeBytes = 0;
                return DokanResult.NotImplemented;
            }
        }

        public NtStatus GetVolumeInformation(out string volumeLabel, out FileSystemFeatures features,
            out string fileSystemName, out uint maximumComponentLength, IDokanFileInfo info)
        {
            try
            {
                Readable.GetVolumeInformation(out volumeLabel, out features, out fileSystemName, out maximumComponentLength);
                return Trace(nameof(GetVolumeInformation), null, info, DokanResult.Success, "out " + volumeLabel,
                    "out " + features.ToString(), "out " + fileSystemName);
            }
            catch
            {
                volumeLabel = "Personal Cloud";
                fileSystemName = "NTFS";
                maximumComponentLength = 256;

                features = FileSystemFeatures.CasePreservedNames | FileSystemFeatures.CaseSensitiveSearch |
                           FileSystemFeatures.PersistentAcls | FileSystemFeatures.SupportsRemoteStorage |
                           FileSystemFeatures.UnicodeOnDisk;

                return Trace(nameof(GetVolumeInformation), null, info, DokanResult.Success, "out " + volumeLabel,
                    "out " + features.ToString(), "out " + fileSystemName);
            }
        }

        public NtStatus GetFileSecurity(string fileName, out FileSystemSecurity security, AccessControlSections sections,
            IDokanFileInfo info)
        {
            security = null;
            return DokanResult.NotImplemented;
        }

        public NtStatus SetFileSecurity(string fileName, FileSystemSecurity security, AccessControlSections sections,
            IDokanFileInfo info)
        {
            return DokanResult.NotImplemented;
        }

        public NtStatus Mounted(IDokanFileInfo info)
        {
            return Trace(nameof(Mounted), null, info, DokanResult.Success);
        }

        public NtStatus Unmounted(IDokanFileInfo info)
        {
            return Trace(nameof(Unmounted), null, info, DokanResult.Success);
        }

        public NtStatus FindStreams(string fileName, IntPtr enumContext, out string streamName, out long streamSize,
            IDokanFileInfo info)
        {
            streamName = string.Empty;
            streamSize = 0;
            return Trace(nameof(FindStreams), fileName, info, DokanResult.NotImplemented, enumContext.ToString(),
                "out " + streamName, "out " + streamSize.ToString(CultureInfo.InvariantCulture));
        }

        public NtStatus FindStreams(string fileName, out IList<FileInformation> streams, IDokanFileInfo info)
        {
            streams = Array.Empty<FileInformation>();
            return Trace(nameof(FindStreams), fileName, info, DokanResult.NotImplemented);
        }

        public IList<FileInformation> FindFilesHelper(string fileName, string searchPattern)
        {
            IList<FileInformation> files = Readable.EnumerateChildren(fileName, searchPattern);
            return files;
        }

        public NtStatus FindFilesWithPattern(string fileName, string searchPattern, out IList<FileInformation> files,
            IDokanFileInfo info)
        {
            if (info is null) throw new ArgumentNullException(nameof(info));

            info.TryResetTimeout(60000);
            files = FindFilesHelper(fileName, searchPattern);
            return Trace(nameof(FindFilesWithPattern), fileName, info, DokanResult.Success);
        }

        #endregion Implementation of IDokanOperations
    }
}
