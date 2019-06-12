class Stack:
    def __init__(self):
        self.items = []

    def is_empty(self):
        return self.items == []

    def push(self, data):
        self.items.append(data)

    def pop(self):
        if len(self.items) < 1:
            return None
        else:
            return self.items.pop()

    def size(self):
        return len(self.items)
