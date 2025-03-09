# Backup
In order to backup the database, simply `xcopy`/`robocopy`/`cp` your `APPVIEWLITE_DIRECTORY` directory, even while AppViewLite is running.
You can exclude the `image-cache` directory (images will be re-downloaded on first use), as well as the `*.cache` files (they will be regenerated on first use).

Metadata on which slices are considered active (not compacted into larger slices yet) is stored in `checkpoints/yyyyMMdd-HHmmss.pb` files. Both these checkpoint files and the slices are written atomically to disk.

Once written, slices are immutable (however, old slices and checkpoints will be periodically garbage collected unless `APPVIEWLITE_DISABLE_SLICE_GC` is set or `APPVIEWLITE_RECENT_CHECKPOINTS_TO_KEEP` is increased).
