# Backup
In order to backup the database, simply `xcopy`/`robocopy`/`cp` your `APPVIEWLITE_DIRECTORY` directory, even while AppViewLite is running.
You can exclude the `image-cache` directory (images will be re-downloaded on first use), as well as the `*.cache` files (they will be regenerated on first use).

Metadata on which slices are considered active (not compacted into larger slices yet) is stored in `checkpoints/yyyyMMdd-HHmmss.pb` files. Both these checkpoint files and the slices are written atomically to disk.

Once written, slices are immutable (however, old slices and checkpoints will be periodically garbage collected unless `APPVIEWLITE_DISABLE_SLICE_GC` is set or `APPVIEWLITE_RECENT_CHECKPOINTS_TO_KEEP` is increased).

Note: If you perform a backup while AppViewLite is running, make sure you perform two or three iterations of the incremental file copy process to catch any new missing files that were written after the initial copy process started.

If, when opening AppViewLite, you receive a `Slice not found: path/1-2-3.col0.dat, referenced by the latest checkpoint file`, you can restore from a backup (don't worry about extra unnecessary files, they will be ignored).

Alternatively, you can resume from an older checkpoint by moving newer checkpoint files out of the `checkpoints/` directory. AppViewLite loads the checkpoint file with the highest Last Modified Time.