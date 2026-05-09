from fluid.utils.dispatcher import Dispatcher


class SimpleDispatcher(Dispatcher[str]):
    def event_type(self, message: str) -> str:
        return "*"


simple = SimpleDispatcher()

assert simple.dispatch("you can dispatch strings to this dispatcher") == 0


def count_words(x: str) -> None:
    words = [x.strip() for w in x.split(" ") if w.strip()]
    print(f"number of words {len(words)}")


simple.register_handler("*", count_words)

assert simple.dispatch("you can dispatch strings to this dispatcher") == 1


def count_letters(x: str) -> None:
    letters = set(x)
    print(f"number of letters {len(letters)}")


simple.register_handler("*.count_letters", count_letters)


assert simple.dispatch("you can dispatch strings to this dispatcher") == 2
