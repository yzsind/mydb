"""SF1 GQL queries for the MySQL protocol adapter used by the official driver."""

from collections import Counter


def forums_with_posts(reader, person_id, min_date, limit):
    friends = set.union(*reader.levels(person_id, 2))
    # Count matched posts inside the database, retaining memberships without posts.
    # A forum can appear in several disjoint person batches.
    rows = reader.by_ids(
        friends,
        f"(p:Person {{id:k}})<-[e:HAS_MEMBER WHERE e.joinDate > {int(min_date)}]-(f:Forum) "
        "OPTIONAL MATCH (p)<-[:HAS_CREATOR]-(post:Post)<-[:CONTAINER_OF]-(f)",
        "f.id f.title COUNT(post)",
    )
    counts = Counter()
    titles = {}
    for forum_id, title, count in rows:
        counts[forum_id] += count
        titles[forum_id] = title
    ranked = sorted(counts, key=lambda key: (-counts[key], key))
    return [(titles[key], counts[key]) for key in ranked[:limit]]


def messages_by_country(reader, parameters):
    from reads import DAY, PERSON, focus, literal

    friends = set.union(*reader.levels(parameters["personIdQ3"], 2))
    start = parameters["startDate"]
    end = start + parameters["durationDays"] * DAY
    x, y = parameters["countryXName"], parameters["countryYName"]
    counts = {}
    for country in dict.fromkeys((x, y)):
        root = focus("Place & Country", country, "c", "name")
        rows = reader.get(
            f"MATCH {root}<-[:IS_LOCATED_IN]-(m:Message WHERE "
            f"m.creationDate >= {start} AND m.creationDate < {end})-[:HAS_CREATOR]->(p:Person)",
            "p.id COUNT(m)",
        )
        counts[country] = {person: count for person, count in rows if person in friends}
    both = counts[x].keys() & counts[y].keys()
    people = reader.by_ids(
        both,
        "(p:Person {id:k})-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->"
        f"(home:Country WHERE home.name <> {literal(x)} AND home.name <> {literal(y)})",
        PERSON,
    )
    results = [
        (person, first, last, counts[x][person], counts[y][person], sum(c[person] for c in counts.values()))
        for person, first, last in people
    ]
    return sorted(results, key=lambda row: (-row[-1], row[0]))[: parameters.get("limit", 20)]


def employees_by_country(reader, parameters):
    from reads import PERSON, focus

    friends = set.union(*reader.levels(parameters["personIdQ11"], 2))
    root = focus("Place & Country", parameters["countryName"], "country", "name")
    rows = reader.get(
        f"MATCH {root}<-[:IS_LOCATED_IN]-(o:Company)"
        f"<-[e:WORK_AT WHERE e.workFrom < {int(parameters['workFromYear'])}]-(p:Person)",
        PERSON + " o.name e.workFrom",
    )
    eligible = [row for row in rows if row[0] in friends]
    eligible.sort(key=lambda row: row[3], reverse=True)
    eligible.sort(key=lambda row: (row[4], row[0]))
    return eligible[: parameters.get("limit", 20)]


def run_bridge():
    """Start the Java driver's Python bridge with the grouped read implementation."""
    import argparse
    import runpy
    import sys
    from pathlib import Path

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--adapter-dir", required=True, type=Path, help="Directory containing bridge.py and reads.py")
    parser.add_argument("--port", type=int, default=14951)
    parser.add_argument("--grouped-only", action="store_true", help="Use the grouped forum query with the original country queries")
    args = parser.parse_args()
    adapter = args.adapter_dir.resolve()
    sys.path.insert(0, str(adapter))
    from reads import Reads

    original = Reads.complex

    def complex_read(reader, number, parameters):
        if number == 3 and not args.grouped_only:
            return messages_by_country(reader, parameters)
        if number == 11 and not args.grouped_only:
            return employees_by_country(reader, parameters)
        if number == 5:
            return forums_with_posts(reader, parameters["personIdQ5"], parameters["minDate"], parameters.get("limit", 20))
        return original(reader, number, parameters)

    Reads.complex = complex_read
    sys.argv = [str(adapter / "bridge.py"), "--port", str(args.port)]
    runpy.run_path(sys.argv[0], run_name="__main__")


if __name__ == "__main__":
    run_bridge()
