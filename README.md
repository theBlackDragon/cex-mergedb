Conan Exiles save database merge tool
=====================================

What this does
--------------
This tool _**attempts**_ to merge two Conan Exiles save game databases, it does this by copying the data from the "child"
database into (a copy of) the master database while rewriting object identifiers from the child database to avoid
conflicts with those already present in the master.

The tool was written to handle single-player games, be they local, or hosted on a dedicated server (the database format
is the same), more specifically it was written to merge the various savegames I accrued over time.

What it does not do
-------------------
 * copy accounts (assumes a single account, which already should be present in all databases)
 * copy guilds
 * modify binary data (this might impact some mods, see also _Known limitations_)

Known limitations
-----------------
 * remapping character IDs breaks Thrall ownership.
   Likely Thrall ownership is stored inside one of the binary blobs in the database. As long as there is no conflict
   between different characters (as in characters from different databases having the same identifier) the tool will
   try to remap conflicting non-character IDs rather than remap characters.

Warning
-------
This was written for my personal use, so any features that I don't use in my saves are likely unsupported, if for no
other reason than that I simply can't test them since they are not present in my save databases.

If you do use this tool make sure to keep backups of your previous saves and be sure to thoroughly test whether
everything is present and works as it should before committing to using a merged save. Especially check whether all the
characters retain Thrall ownership.
