"""Combined verification entry: original unittest suite plus switch scenario."""
from pathlib import Path
import sys
from tempfile import TemporaryDirectory

import check_project
from switch_run import check_switch


def main():
    status = check_project.main()
    if status != 0:
        return status
    with TemporaryDirectory() as directory:
        try:
            check_switch(Path(directory))
        except BaseException as exc:  # noqa: BLE001 - report any switch failure
            print(f'SWITCH VERIFY ERROR {type(exc).__name__}: {exc}', file=sys.stderr)
            return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
