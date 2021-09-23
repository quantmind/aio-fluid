import sys

length = 2 ** 17 + 2 ** 10


if __name__ == "__main__":
    lines = int(sys.argv[1]) if len(sys.argv) == 2 else 1
    text = "c" * length
    for _ in range(lines):
        print(f"{text}")
