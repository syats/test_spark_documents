def count_words(x, debug=True):
    sp = x.split()
    if debug:
        print("\n\n"+"->"+sp[0], len(x), "\n")
    return sp.map(lambda word: (word, 1))


