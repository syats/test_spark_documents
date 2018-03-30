def count_words(x, debug=True):
    sp = x.split()
    if debug:
        print("\n\n", "->", len(x), " :: ", x, "\n")
    return [(word, 1) for word in sp]


