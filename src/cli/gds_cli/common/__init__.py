import shutil

# Console width shared across the CLI so rules, tables, and dividers render at a
# consistent, readable line length instead of stretching across a wide terminal.
# Default 100, but shrink to the terminal width when it is narrower so output
# never overflows a small terminal. (get_terminal_size falls back to 80 columns
# when there is no tty, e.g. when output is piped.)
CONSOLE_WIDTH = min(100, shutil.get_terminal_size().columns)
