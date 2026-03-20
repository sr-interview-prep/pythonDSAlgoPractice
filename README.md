# VS Code Extension Setup

Install the local VS Code extension using Make commands from the repository root:

```bash
make ext-deps
make ext-build
make ext-package
make ext-install
```

One-command install (runs build + package + install):

```bash
make ext-install
```

If you want to install into a specific IDE CLI:

```bash
make ext-install IDE_CMD=/Applications/Cursor.app/Contents/Resources/app/bin/cursor
make ext-install IDE_CMD=code
```

## VS Code Extension Shortcuts

After installation, use these shortcuts:

- `Cmd+Alt+T` or `Cmd+Shift+R`: Run test for the current file in `sql_pyspark`
- `Cmd+Alt+Shift+T` or `Cmd+Shift+Alt+R`: Run all SQL PySpark tests
