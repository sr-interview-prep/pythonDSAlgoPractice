import * as path from "path";
import * as vscode from "vscode";

const SUPPORTED_ROOTS = ["sql_pyspark/", "data_manipulation/"] as const;

function shellQuote(value: string): string {
  return `'${value.replace(/'/g, `'\\''`)}'`;
}

function toProblemTarget(workspaceRoot: string, filePath: string): string | undefined {
  const relative = path.relative(workspaceRoot, filePath).split(path.sep).join("/");
  for (const root of SUPPORTED_ROOTS) {
    if (relative.startsWith(root)) {
      const withoutPrefix = relative.slice(root.length);
      return withoutPrefix.replace(/\.(sql|py|scala|test\.json)$/i, "");
    }
  }
  return undefined;
}

function getOrCreateTerminal(workspaceRoot: string): vscode.Terminal {
  const name = "SQL PySpark Tests";
  const existing = vscode.window.terminals.find((t) => t.name === name);
  if (existing) {
    return existing;
  }
  return vscode.window.createTerminal({ name, cwd: workspaceRoot });
}

function runMakeCommand(workspaceRoot: string, command: string): void {
  const terminal = getOrCreateTerminal(workspaceRoot);
  terminal.show(true);
  terminal.sendText(command, true);
}

export function activate(context: vscode.ExtensionContext): void {
  const runForCurrent = vscode.commands.registerCommand(
    "sqlPysparkTests.runForCurrentFile",
    () => {
      const workspaceRoot = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
      const activePath = vscode.window.activeTextEditor?.document.uri.fsPath;

      if (!workspaceRoot || !activePath) {
        void vscode.window.showErrorMessage(
          "Open a workspace and file under sql_pyspark or data_manipulation first."
        );
        return;
      }

      const target = toProblemTarget(workspaceRoot, activePath);
      if (!target) {
        void vscode.window.showErrorMessage(
          "Current file must be inside sql_pyspark or data_manipulation."
        );
        return;
      }

      runMakeCommand(workspaceRoot, `make test ${shellQuote(target)}`);
      void vscode.window.showInformationMessage(`Running: make test ${target}`);
    }
  );

  const runAll = vscode.commands.registerCommand("sqlPysparkTests.runAll", () => {
    const workspaceRoot = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
    if (!workspaceRoot) {
      void vscode.window.showErrorMessage("Open the repository workspace first.");
      return;
    }
    runMakeCommand(workspaceRoot, "make test-all");
    void vscode.window.showInformationMessage("Running all data manipulation tests.");
  });

  context.subscriptions.push(runForCurrent, runAll);
}

export function deactivate(): void {}
