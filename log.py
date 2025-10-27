import time
from pathlib import Path

from _config import Config


class Log:
    _log_path = None
    _open_error_reported = False
    _write_error_reported = False

    @staticmethod
    def print(info):
        if Config.debug:
            if not info.endswith('\n'):
                info += '\n'
            print(info)

    @staticmethod
    def write(info, host=None):
        print(f'*** {info} ***\n\n')
        if host == '127.0.0.1':
            return

        Log._append_to_file(info)

    @staticmethod
    def _resolve_path() -> Path:
        if Log._log_path is not None:
            return Log._log_path

        fallback = Path('python-rtsp-server.log').resolve()
        configured = getattr(Config, 'log_file', None)

        candidates = []
        if configured:
            candidates.append(Path(configured).expanduser())
        candidates.append(fallback)

        for path in candidates:
            try:
                if path.parent and not path.parent.exists():
                    path.parent.mkdir(parents=True, exist_ok=True)
                with path.open('a', encoding='utf-8'):
                    pass
            except (OSError, PermissionError):
                if not Log._open_error_reported:
                    Log.print(f'Log: warning: unable to open log file "{path}". '
                              f'Falling back to "{fallback}".')
                    Log._open_error_reported = True
                continue

            Log._log_path = path.resolve()
            return Log._log_path

        Log._log_path = fallback
        return Log._log_path

    @staticmethod
    def _append_to_file(info: str) -> None:
        path = Log._resolve_path()
        text = f'{time.strftime("%Y-%m-%d %H:%M:%S")} {info}'
        try:
            with path.open('a', encoding='utf-8') as handle:
                handle.write(text + '\n')
        except (OSError, PermissionError) as exc:
            if not Log._write_error_reported:
                Log.print(f'Log: warning: unable to write log file "{path}": {exc}')
                Log._write_error_reported = True
