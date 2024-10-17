import pandas as pd
import asyncio
from rapidfuzz import process, fuzz
import datetime

FILE_EXTENSION = ".xlsx"
FOLFER_DOCUMENTS = "documents/"
RESULTS_FOLDER = "results/"

async def fuzz_matching(reference: str, list_to_search: list) -> list:
    results = process.extract(reference, list_to_search, scorer=fuzz.ratio, limit=None, score_cutoff=80)
    return [{
        "reference": reference,
        "found": match,
        "ratio": score
    } for match, score, _ in results]

async def compare_documents():
    print("Working on comparing multiple documents")
    distribution = {"document1": {}, "document2": {}}
    try:
        print("Information for the first document")
        distribution["document1"] = {
            "doc_name": f"{FOLFER_DOCUMENTS}{input('Name of the first document: ')}{FILE_EXTENSION}",
            "column_reference": input("Enter the name of the column to analyze: "),
            "search_doc": input("Is this the reference document (Y/N): ")
        }
        distribution["document2"] = {
            "doc_name": f"{FOLFER_DOCUMENTS}{input('Name of the second document: ')}{FILE_EXTENSION}",
            "column_reference": input("Enter the name of the column to analyze: "),
            "search_doc": "Y" if distribution["document1"]["search_doc"] == "N" else "N"
        }
        print("Loading documents...")

        document_1 = pd.read_excel(distribution["document1"]["doc_name"])
        document_2 = pd.read_excel(distribution["document2"]["doc_name"])
        print("Documents loaded.")

        if distribution["document1"]["search_doc"] == "Y":
            cfn_to_search = list(document_1[distribution["document1"]["column_reference"]].dropna().unique())
            search_on_this_list = list(document_2[distribution["document2"]["column_reference"]].dropna().unique())
        else:
            print("The second document is the reference.")
            cfn_to_search = list(document_2[distribution["document2"]["column_reference"]].dropna().unique())
            search_on_this_list = list(document_1[distribution["document1"]["column_reference"]].dropna().unique())
        print(datetime.datetime.now())
        results = await asyncio.gather(*[fuzz_matching(cfn, search_on_this_list) for cfn in cfn_to_search])
        final_result = [item for sublist in results for item in sublist]
        print(datetime.datetime.now())
        output = pd.DataFrame(final_result)
        output.to_excel(f"{RESULTS_FOLDER}comparation{FILE_EXTENSION}")

    except (FileNotFoundError, ValueError) as e:
        print("Error comparing documents:")
        print(e)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
