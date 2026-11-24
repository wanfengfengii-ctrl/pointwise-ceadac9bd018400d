"""Demo entry: original receipt demo, then the two-site switch demonstration."""
from pathlib import Path
from tempfile import TemporaryDirectory

import demo
from switch_run import demo_switch


def main():
    status = demo.main()
    if status != 0:
        return status
    print()
    print('Two-site day switch:')
    with TemporaryDirectory() as directory:
        return demo_switch(Path(directory))


if __name__ == '__main__':
    raise SystemExit(main())
