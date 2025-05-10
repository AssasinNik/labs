import sys
import os
import time
import shutil
import threading
from enum import Enum
from datetime import datetime

class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    RESET = '\033[0m'
    BOLD = '\033[1m'
    DIM = '\033[2m'

class OperationType(Enum):
    INFO = 1
    PROGRESS = 2
    SUCCESS = 3
    WARNING = 4
    ERROR = 5
    TABLE = 6

class ProgressBar:
    def __init__(self, total=100, prefix='', suffix='', bar_length=30,
                 fill_char='█', empty_char='░', start_char='', end_char=''):
        self.total = total
        self.prefix = prefix
        self.suffix = suffix
        self.bar_length = bar_length
        self.fill_char = fill_char
        self.empty_char = empty_char
        self.start_char = start_char
        self.end_char = end_char
        self.current = 0
        self.start_time = time.time()
        self.last_update_time = self.start_time
        self.last_current = 0
        self.speed = 0
        self.eta = "??:??"

    def update(self, current):
        if current > self.total:
            current = self.total

        self.current = current
        now = time.time()
        elapsed = now - self.last_update_time

        if elapsed > 0.1:
            items_done = current - self.last_current
            self.speed = items_done / elapsed if elapsed > 0 else 0
            self.last_update_time = now
            self.last_current = current

            if self.speed > 0 and self.total > 0:
                remaining_seconds = (self.total - self.current) / self.speed
                minutes = int(remaining_seconds // 60)
                seconds = int(remaining_seconds % 60)
                self.eta = f"{minutes:02d}:{seconds:02d}"
            else:
                self.eta = "??:??"

    def get_progress_bar(self, width):
        if self.total == 0:
            return f"{Colors.YELLOW}Waiting for data...{Colors.RESET}"

        percentage = 100 * (self.current / float(self.total))
        filled_length = int(self.bar_length * self.current // self.total)

        bar = self.fill_char * filled_length
        empty = self.empty_char * (self.bar_length - filled_length)
        progress_bar = f"{self.start_char}{bar}{empty}{self.end_char}"

        suffix = f"{self.current}/{self.total} ({percentage:.1f}%) | {self.speed:.1f} i/s | ETA: {self.eta}"
        available_width = width - len(self.prefix) - len(suffix) - 4

        if available_width > 0:
            progress_bar = progress_bar[:available_width]

        return f"{Colors.CYAN}{self.prefix}{Colors.RESET} {progress_bar} {Colors.DIM}{suffix}{Colors.RESET}"

class TerminalVisualizer:
    def __init__(self):
        self.operations = {}
        self.operations_order = []
        self.terminal_width = self._get_terminal_width()
        self.terminal_height = self._get_terminal_height()
        self.last_lines_count = 0
        self.start_time = time.time()
        self.operation_stats = {}
        self.max_operation_lines = 0

    def _get_terminal_width(self):
        try:
            return max(80, shutil.get_terminal_size().columns)
        except:
            return 80

    def _get_terminal_height(self):
        try:
            return shutil.get_terminal_size().lines
        except:
            return 24

    def _calculate_layout(self):
        self.terminal_width = self._get_terminal_width()
        self.terminal_height = self._get_terminal_height()
        self.max_operation_lines = max(0, self.terminal_height - 8)

    def start_operation(self, operation_name, operation_type=OperationType.PROGRESS, total=100):
        self._calculate_layout()

        if operation_name not in self.operations:
            self.operations[operation_name] = {
                "type": operation_type,
                "progress_bar": ProgressBar(
                    total=total,
                    prefix=operation_name,
                    bar_length=20
                ),
                "start_time": time.time(),
                "end_time": None,
                "items_processed": 0
            }
            self.operations_order.append(operation_name)
            self.operation_stats[operation_name] = {
                "start_time": time.time(),
                "total": total,
                "end_time": None,
                "items_processed": 0
            }

        self._render()
        return operation_name

    def update_progress(self, operation=None, current=None, total=None, increment=None):
        if operation is None:
            return

        op = self.operations.get(operation)
        if not op:
            return

        bar = op["progress_bar"]

        if total is not None:
            bar.total = total

        if increment is not None:
            new_current = bar.current + increment
            bar.update(new_current)
            op["items_processed"] = new_current
        elif current is not None:
            bar.update(current)
            op["items_processed"] = current

        self.operation_stats[operation]["items_processed"] = bar.current
        self._render()

    def complete_operation(self, operation=None, success=True):
        if operation is None:
            return

        op = self.operations.get(operation)
        if not op:
            return

        op["end_time"] = time.time()
        op["progress_bar"].update(op["progress_bar"].total)
        op["type"] = OperationType.SUCCESS if success else OperationType.WARNING
        self.operation_stats[operation]["end_time"] = op["end_time"]
        self._render()

    def log_info(self, message):
        operation_id = f"INFO:{time.time()}"
        self.operations[operation_id] = {
            "type": OperationType.INFO,
            "message": message,
            "time": time.time()
        }
        self.operations_order.append(operation_id)
        self._render()

    def _clear_previous_output(self):
        if self.last_lines_count > 0:
            sys.stdout.write(f"\033[{self.last_lines_count}A\033[J")
            sys.stdout.flush()

    def _render_header(self):
        current_time = datetime.now().strftime("%H:%M:%S")
        elapsed = time.time() - self.start_time
        elapsed_str = time.strftime("%H:%M:%S", time.gmtime(elapsed))

        header = f"{Colors.BOLD}Data Generation Process{Colors.RESET} [{current_time}] {Colors.DIM}Elapsed: {elapsed_str}{Colors.RESET}"
        header_width = len(header) - len(Colors.BOLD) - len(Colors.RESET)*2 - len(Colors.DIM)*2
        padding = max(0, (self.terminal_width - header_width) // 2)

        lines = [
            f"{Colors.DIM}{'=' * self.terminal_width}{Colors.RESET}",
            f"{' ' * padding}{header}",
            f"{Colors.DIM}{'=' * self.terminal_width}{Colors.RESET}"
        ]
        return lines

    def _render_operations(self):
        lines = []
        visible_operations = self.operations_order[-self.max_operation_lines:] if len(self.operations_order) > self.max_operation_lines else self.operations_order

        col1_width = int(self.terminal_width * 0.35)
        col2_width = self.terminal_width - col1_width - 3

        for op_name in visible_operations:
            op = self.operations.get(op_name)
            if not op:
                continue

            if op["type"] == OperationType.PROGRESS:
                progress_bar = op["progress_bar"].get_progress_bar(col2_width)
                lines.append(f"{Colors.WHITE}{op_name[:col1_width]:<{col1_width}}{Colors.RESET} {progress_bar}")
            elif op["type"] == OperationType.INFO:
                lines.append(f"{Colors.CYAN}INFO:{Colors.RESET} {op['message']}")
            elif op["type"] == OperationType.SUCCESS:
                duration = op["end_time"] - op["start_time"]
                lines.append(f"{Colors.GREEN}✓ {op_name[:col1_width]}{Colors.RESET} {Colors.DIM}Completed in {duration:.2f}s ({op['items_processed']} items){Colors.RESET}")
            elif op["type"] == OperationType.ERROR:
                lines.append(f"{Colors.RED}✗ {op_name[:col1_width]}{Colors.RESET} {Colors.DIM}{op['message']}{Colors.RESET}")

        # Fill remaining lines
        while len(lines) < self.max_operation_lines:
            lines.append("")

        return lines

    def _render(self):
        self._clear_previous_output()
        self._calculate_layout()

        lines = []
        lines += self._render_header()
        lines += self._render_operations()
        lines.append(f"{Colors.DIM}{'=' * self.terminal_width}{Colors.RESET}")

        output = "\n".join(lines)
        print(output)
        self.last_lines_count = len(lines)

    def show_summary(self):
        self._clear_previous_output()
        total_time = time.time() - self.start_time
        total_items = sum(stat["items_processed"] for stat in self.operation_stats.values())

        lines = [
            f"{Colors.DIM}{'=' * self.terminal_width}{Colors.RESET}",
            f"{Colors.BOLD}Data Generation Summary{Colors.RESET}".center(self.terminal_width),
            f"{Colors.DIM}{'=' * self.terminal_width}{Colors.RESET}"
        ]

        # Table headers
        headers = [
            f"{'Operation':<30}",
            f"{'Items':>10}",
            f"{'Time':>8}",
            f"{'Speed':>10}",
            f"{'% Time':>8}"
        ]
        lines.append(f"{Colors.BOLD}{' | '.join(headers)}{Colors.RESET}")
        lines.append(f"{Colors.DIM}{'-' * self.terminal_width}{Colors.RESET}")

        # Table rows
        for op_name, stats in self.operation_stats.items():
            if stats["end_time"] is None:
                continue

            op_time = stats["end_time"] - stats["start_time"]
            items = stats["items_processed"]
            speed = items / op_time if op_time > 0 else 0
            time_percent = (op_time / total_time * 100) if total_time > 0 else 0

            line = (
                f"{op_name[:30]:<30}",
                f"{items:>10,}",
                f"{op_time:>8.2f}",
                f"{speed:>10.1f}",
                f"{time_percent:>7.1f}%"
            )
            lines.append(" | ".join(line))

        # Footer
        lines.append(f"{Colors.DIM}{'=' * self.terminal_width}{Colors.RESET}")
        lines.append(f"{Colors.BOLD}Total time:{Colors.RESET} {time.strftime('%H:%M:%S', time.gmtime(total_time))} "
                    f"{Colors.DIM}|{Colors.RESET} {Colors.BOLD}Total items:{Colors.RESET} {total_items:,}")
        lines.append(f"{Colors.DIM}{'=' * self.terminal_width}{Colors.RESET}")

        print("\n".join(lines))

# Глобальный объект визуализатора
visualizer = TerminalVisualizer()

# API для использования в скрипте
def info(message):
    visualizer.log_info(message)

def start_operation(name, total=100):
    return visualizer.start_operation(name, OperationType.PROGRESS, total)

def update_progress(operation=None, current=None, total=None, increment=None):
    visualizer.update_progress(operation, current, total, increment)

def complete_operation(operation=None, success=True):
    visualizer.complete_operation(operation, success)

def show_summary():
    visualizer.show_summary()