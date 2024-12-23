def main():
    doc1 = store.get("doc1")
    doc2 = store.get("doc2")
    len1 = len(doc1)
    len2 = len(doc2)
    if len1 > len2:
        store.put("func1_doc", doc1)
    else:
        store.put("func1_doc", doc2)
