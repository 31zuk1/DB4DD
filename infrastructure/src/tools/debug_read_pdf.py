import fitz
import sys

def read_pdf(path):
    doc = fitz.open(path)
    text = ""
    for page in doc:
        text += page.get_text() + "\n"
    return text

if __name__ == "__main__":
    print(read_pdf(sys.argv[1]))
