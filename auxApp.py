def count_words(x, debug=True):
    sp = x[1].split()
    if debug:
        print("\n\n"+"->"+sp[0], len(x[1]), "\n")
    return len(sp)

