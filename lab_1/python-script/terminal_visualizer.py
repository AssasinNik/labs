import sys
import os
import time
import shutil
import threading
from enum import Enum
from datetime import datetime

class OperationType(Enum):
    INFO = 1
    PROGRESS = 2
    SUCCESS = 3
    WARNING = 4
    ERROR = 5
    TABLE = 6

class Spinner:
    def __init__(self):
        self.spinners = [
            ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"],
            ["-", "\\", "|", "/"],
            ["▁", "▂", "▃", "▄", "▅", "▆", "▇", "█", "▇", "▆", "▅", "▄", "▃", "▂"],
            ["←", "↖", "↑", "↗", "→", "↘", "↓", "↙"],
            ["▉", "▊", "▋", "▌", "▍", "▎", "▏", "▎", "▍", "▌", "▋", "▊", "▉"],
        ]
        self.current_spinner = 0
        self.index = 0
        self.active = False
        self.thread = None

    def get_next(self):
        chars = self.spinners[self.current_spinner]
        char = chars[self.index % len(chars)]
        self.index += 1
        return char

    def start(self):
        self.active = True
        self.thread = threading.Thread(target=self._spin)
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        self.active = False
        if self.thread:
            self.thread.join(0.1)

    def _spin(self):
        while self.active:
            time.sleep(0.1)

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
        
        if elapsed > 0.1:  # Обновляем скорость не чаще чем раз в 0.1 сек
            items_done = current - self.last_current
            self.speed = items_done / elapsed if elapsed > 0 else 0
            self.last_update_time = now
            self.last_current = current
            
            # Расчет ETA
            if self.speed > 0:
                remaining_seconds = (self.total - self.current) / self.speed
                minutes = int(remaining_seconds // 60)
                seconds = int(remaining_seconds % 60)
                self.eta = f"{minutes:02d}:{seconds:02d}"
            else:
                self.eta = "??:??"

    def get_progress_bar(self):
        percentage = 100 * (self.current / float(self.total))
        filled_length = int(self.bar_length * self.current // self.total)
        
        # Создаем прогресс-бар
        bar = self.fill_char * filled_length
        empty = self.empty_char * (self.bar_length - filled_length)
        progress_bar = f"{self.start_char}{bar}{empty}{self.end_char}"
        
        # Добавляем процент, скорость и ETA
        suffix = f"{self.suffix} {percentage:3.0f}% {self.speed:.1f} items/s ETA:{self.eta}"
        
        return f"{self.prefix} {progress_bar} {suffix}"
    
    def get_pulsating_bar(self):
        # Создаем пульсирующий индикатор для операций с неизвестным временем
        bar_length = self.bar_length
        elapsed = time.time() - self.start_time
        position = int((elapsed * 10) % (2 * bar_length))
        
        if position > bar_length:
            position = 2 * bar_length - position
            
        bar = self.empty_char * (position - 1)
        if position > 0:
            bar += self.fill_char
        bar += self.empty_char * (bar_length - position)
        
        return f"{self.prefix} {self.start_char}{bar}{self.end_char} {self.suffix}"


class TerminalVisualizer:
    def __init__(self):
        self.current_operation = None
        self.operations = {}
        self.operations_order = []
        self.terminal_width = self._get_terminal_width()
        self.spinner = Spinner()
        self.last_lines_count = 0
        self.start_time = time.time()
        
        # Сохраняем метрики операций для финальной сводки
        self.operation_stats = {}

    def _get_terminal_width(self):
        try:
            columns, _ = shutil.get_terminal_size()
            return max(80, columns)
        except:
            return 80

    def start_operation(self, operation_name, operation_type=OperationType.PROGRESS, total=100):
        """Начать новую операцию с прогресс-баром"""
        self.terminal_width = self._get_terminal_width()
        bar_length = min(30, max(10, int(self.terminal_width * 0.3)))
        
        if operation_name in self.operations:
            # Если операция существует, просто обновляем тип и сбрасываем прогресс
            op = self.operations[operation_name]
            op["type"] = operation_type
            op["progress_bar"] = ProgressBar(
                total=total, 
                prefix=operation_name, 
                suffix="", 
                bar_length=bar_length
            )
            op["start_time"] = time.time()
        else:
            # Создаем новую операцию
            self.operations[operation_name] = {
                "type": operation_type,
                "progress_bar": ProgressBar(
                    total=total, 
                    prefix=operation_name, 
                    suffix="", 
                    bar_length=bar_length
                ),
                "start_time": time.time(),
                "end_time": None,
                "items_processed": 0
            }
            self.operations_order.append(operation_name)
        
        self.current_operation = operation_name
        self._render()
        
        # Записываем начальную статистику
        self.operation_stats[operation_name] = {
            "start_time": time.time(),
            "total": total,
            "end_time": None,
            "items_processed": 0
        }
        
        return operation_name

    def update_progress(self, operation_name=None, current=None, total=None, increment=None):
        """Обновить прогресс операции"""
        if operation_name is None:
            operation_name = self.current_operation
            
        if operation_name not in self.operations:
            return
            
        op = self.operations[operation_name]
        bar = op["progress_bar"]
        
        if total is not None:
            bar.total = total
            
        if increment is not None:
            bar.update(bar.current + increment)
            op["items_processed"] = bar.current
        elif current is not None:
            bar.update(current)
            op["items_processed"] = current
            
        # Обновляем статистику
        self.operation_stats[operation_name]["items_processed"] = bar.current
        
        self._render()

    def complete_operation(self, operation_name=None, success=True):
        """Завершить операцию"""
        if operation_name is None:
            operation_name = self.current_operation
            
        if operation_name not in self.operations:
            return
            
        op = self.operations[operation_name]
        op["end_time"] = time.time()
        
        # Завершаем прогресс
        bar = op["progress_bar"]
        bar.update(bar.total)
        
        op["type"] = OperationType.SUCCESS if success else OperationType.WARNING
        
        # Обновляем статистику
        self.operation_stats[operation_name]["end_time"] = time.time()
        self.operation_stats[operation_name]["items_processed"] = bar.total
        
        self._render()

    def log_info(self, message):
        """Записать информационное сообщение"""
        operation_name = f"INFO: {message}"
        self.operations[operation_name] = {
            "type": OperationType.INFO,
            "message": message,
            "time": time.time()
        }
        self.operations_order.append(operation_name)
        self._render()

    def log_error(self, message):
        """Записать сообщение об ошибке"""
        operation_name = f"ERROR: {message}"
        self.operations[operation_name] = {
            "type": OperationType.ERROR,
            "message": message,
            "time": time.time()
        }
        self.operations_order.append(operation_name)
        self._render()

    def _clear_previous_output(self):
        """Очистить предыдущий вывод"""
        if self.last_lines_count > 0:
            sys.stdout.write(f"\033[{self.last_lines_count}A")  # Перемещаем курсор вверх
            sys.stdout.write("\033[J")  # Очищаем всё до конца экрана
            sys.stdout.flush()

    def _render(self):
        """Отрисовать текущий статус всех операций"""
        self._clear_previous_output()
        
        lines = []
        
        # Заголовок
        current_time = datetime.now().strftime("%H:%M:%S")
        elapsed = time.time() - self.start_time
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)
        
        header = f"[Data Generation Process - {current_time} - Elapsed time: {minutes:02d}:{seconds:02d}]"
        padding = (self.terminal_width - len(header)) // 2
        lines.append("=" * self.terminal_width)
        lines.append(" " * padding + header)
        lines.append("=" * self.terminal_width)
        
        # Таблица операций
        table_width = self.terminal_width - 4
        col1_width = int(table_width * 0.3)
        col2_width = table_width - col1_width

        lines.append(f"| {'Operation':<{col1_width}} | {'Progress':<{col2_width-2}} |")
        lines.append(f"|{'-'*(col1_width+2)}|{'-'*col2_width}|")
        
        # Отображаем последние 10 операций
        visible_operations = self.operations_order[-10:] if len(self.operations_order) > 10 else self.operations_order
        
        for op_name in visible_operations:
            op = self.operations[op_name]
            
            if op["type"] == OperationType.PROGRESS:
                progress_text = op["progress_bar"].get_progress_bar()
                lines.append(f"| {op_name[:col1_width]:<{col1_width}} | {progress_text[:col2_width-2]:<{col2_width-2}} |")
            elif op["type"] == OperationType.INFO:
                lines.append(f"| {'INFO':<{col1_width}} | {op['message'][:col2_width-2]:<{col2_width-2}} |")
            elif op["type"] == OperationType.SUCCESS:
                duration = op["end_time"] - op["start_time"]
                lines.append(f"| {op_name[:col1_width]:<{col1_width}} | {'[COMPLETED]'} in {duration:.2f}s ({op['items_processed']} items) |")
            elif op["type"] == OperationType.WARNING:
                lines.append(f"| {op_name[:col1_width]:<{col1_width}} | {'[WARNING]'} {op['message'][:col2_width-12]:<{col2_width-12}} |")
            elif op["type"] == OperationType.ERROR:
                lines.append(f"| {op_name[:col1_width]:<{col1_width}} | {'[ERROR]'} {op['message'][:col2_width-10]:<{col2_width-10}} |")
        
        lines.append("=" * self.terminal_width)
        
        output = "\n".join(lines)
        print(output)
        self.last_lines_count = len(lines)
        
    def show_summary(self):
        """Показать сводную таблицу результатов"""
        self._clear_previous_output()
        
        total_time = time.time() - self.start_time
        total_items = sum(stat["items_processed"] for stat in self.operation_stats.values())
        minutes = int(total_time // 60)
        seconds = total_time % 60
        
        lines = []
        lines.append("=" * self.terminal_width)
        header = "[Data Generation Summary]"
        padding = (self.terminal_width - len(header)) // 2
        lines.append(" " * padding + header)
        lines.append("=" * self.terminal_width)
        
        # Таблица статистики
        table_width = self.terminal_width - 4
        col1_width = int(table_width * 0.3)
        col2_width = int(table_width * 0.15)
        col3_width = int(table_width * 0.15)
        col4_width = int(table_width * 0.15)
        col5_width = table_width - col1_width - col2_width - col3_width - col4_width
        
        lines.append(f"| {'Operation':<{col1_width}} | {'Items':<{col2_width}} | {'Time (s)':<{col3_width}} | {'Speed (i/s)':<{col4_width}} | {'% of Total':<{col5_width}} |")
        lines.append(f"|{'-'*(col1_width+2)}|{'-'*(col2_width+2)}|{'-'*(col3_width+2)}|{'-'*(col4_width+2)}|{'-'*(col5_width+2)}|")
        
        for op_name, stats in self.operation_stats.items():
            if stats["end_time"] is not None:
                op_time = stats["end_time"] - stats["start_time"]
                items = stats["items_processed"]
                speed = items / op_time if op_time > 0 else 0
                percentage = (op_time / total_time * 100) if total_time > 0 else 0
                
                lines.append(f"| {op_name[:col1_width]:<{col1_width}} | {items:<{col2_width}} | {op_time:.2f}{'':^{col3_width-7}} | {speed:.2f}{'':^{col4_width-7}} | {percentage:.1f}%{'':^{col5_width-6}} |")
        
        lines.append("=" * self.terminal_width)
        lines.append(f"Total execution time: {minutes}m {seconds:.2f}s | Total items processed: {total_items}")
        lines.append("=" * self.terminal_width)
        
        output = "\n".join(lines)
        print(output)


# Глобальный объект визуализатора для использования в скрипте
visualizer = TerminalVisualizer()

# API для использования в основном скрипте (замена логгера)
def info(message):
    visualizer.log_info(message)

def error(message):
    visualizer.log_error(message)

def start_operation(name, total=100):
    return visualizer.start_operation(name, OperationType.PROGRESS, total)

def update_progress(operation=None, current=None, total=None, increment=None):
    visualizer.update_progress(operation, current, total, increment)

def complete_operation(operation=None, success=True):
    visualizer.complete_operation(operation, success)

def show_summary():
    visualizer.show_summary() 