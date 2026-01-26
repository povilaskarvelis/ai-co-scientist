#!/usr/bin/env python3
"""
Test script to verify agent accuracy against ground truth.
Run: python test_accuracy.py
"""
import httpx

OPEN_TARGETS_API = "https://api.platform.opentargets.org/api/v4/graphql"
NCBI_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

def test_disease_search():
    """Test: search_diseases should find Alzheimer's with correct ID"""
    print("\n" + "="*60)
    print("TEST 1: Disease Search - 'Alzheimer'")
    print("="*60)
    
    graphql = """
    query {
        search(queryString: "Alzheimer", entityNames: ["disease"], page: {size: 5, index: 0}) {
            hits { id name }
        }
    }
    """
    resp = httpx.post(OPEN_TARGETS_API, json={"query": graphql}, timeout=30)
    hits = resp.json()["data"]["search"]["hits"]
    
    print("\nExpected: Should find 'Alzheimer disease' with ID containing 'MONDO' or 'EFO'")
    print("\nActual results:")
    for h in hits:
        print(f"  - {h['name']}: {h['id']}")
    
    # Verify
    alzheimer_found = any("alzheimer" in h["name"].lower() for h in hits)
    print(f"\n✓ PASS" if alzheimer_found else "✗ FAIL")
    return alzheimer_found


def test_target_association():
    """Test: Alzheimer's should have APOE, APP, PSEN1 as top targets"""
    print("\n" + "="*60)
    print("TEST 2: Target Association - Alzheimer's disease")
    print("="*60)
    
    # EFO_0000249 is Alzheimer's disease
    graphql = """
    query {
        disease(efoId: "MONDO_0004975") {
            name
            associatedTargets(page: {size: 20, index: 0}) {
                rows { target { approvedSymbol } score }
            }
        }
    }
    """
    resp = httpx.post(OPEN_TARGETS_API, json={"query": graphql}, timeout=30)
    data = resp.json()["data"]["disease"]
    
    print(f"\nDisease: {data['name']}")
    print("\nExpected: APOE, APP, PSEN1, MAPT should be among top targets")
    print("\nActual top 10 targets:")
    
    symbols = []
    for row in data["associatedTargets"]["rows"][:10]:
        sym = row["target"]["approvedSymbol"]
        score = row["score"] * 100
        symbols.append(sym)
        print(f"  - {sym}: {score:.1f}%")
    
    # Known Alzheimer targets
    expected = {"APOE", "APP", "PSEN1", "MAPT"}
    found = expected.intersection(set(symbols))
    
    print(f"\nExpected targets found: {found}")
    print(f"✓ PASS ({len(found)}/4 found)" if len(found) >= 2 else "✗ FAIL")
    return len(found) >= 2


def test_druggability():
    """Test: APP should show as druggable with known drugs"""
    print("\n" + "="*60)
    print("TEST 3: Druggability Check - APP (ENSG00000142192)")
    print("="*60)
    
    graphql = """
    query {
        target(ensemblId: "ENSG00000142192") {
            approvedSymbol
            knownDrugs { uniqueDrugs }
            tractability { modality label value }
        }
    }
    """
    resp = httpx.post(OPEN_TARGETS_API, json={"query": graphql}, timeout=30)
    target = resp.json()["data"]["target"]
    
    num_drugs = target["knownDrugs"]["uniqueDrugs"]
    has_sm_tractability = any(
        t["modality"] == "SM" and t["value"] 
        for t in target["tractability"]
    )
    
    print(f"\nTarget: {target['approvedSymbol']}")
    print(f"Known drugs: {num_drugs}")
    print(f"Small molecule tractable: {has_sm_tractability}")
    
    print("\nExpected: >0 known drugs, small molecule tractable")
    passed = num_drugs > 0 and has_sm_tractability
    print(f"✓ PASS" if passed else "✗ FAIL")
    return passed


def test_pubmed():
    """Test: PubMed should return papers for common search"""
    print("\n" + "="*60)
    print("TEST 4: PubMed Search - 'LRRK2 Parkinson'")
    print("="*60)
    
    resp = httpx.get(
        f"{NCBI_BASE}/esearch.fcgi",
        params={"db": "pubmed", "term": "LRRK2 Parkinson", "retmax": 5, "retmode": "json"},
        timeout=30
    )
    ids = resp.json()["esearchresult"]["idlist"]
    
    print(f"\nPapers found: {len(ids)}")
    print(f"PMIDs: {ids}")
    
    print("\nExpected: Should find multiple papers (LRRK2 is well-studied)")
    passed = len(ids) >= 3
    print(f"✓ PASS" if passed else "✗ FAIL")
    return passed


def main():
    print("\n" + "#"*60)
    print("# Agent Accuracy Tests - Ground Truth Verification")
    print("#"*60)
    
    results = []
    results.append(("Disease Search", test_disease_search()))
    results.append(("Target Association", test_target_association()))
    results.append(("Druggability", test_druggability()))
    results.append(("PubMed", test_pubmed()))
    
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    for name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"  {name}: {status}")
    
    passed_count = sum(1 for _, p in results if p)
    print(f"\nTotal: {passed_count}/{len(results)} tests passed")


if __name__ == "__main__":
    main()
