"""Interactive read plans over an actual versioned graph export."""

from bisect import bisect_left, insort
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from time import perf_counter

from forum import ForumActivity
from paths import shortest
from ranked import NeighborhoodTopK

DAY = 86_400_000


@dataclass(slots=True)
class Message:
    when: int
    body: str | None
    post: bool
    author: int | None = None
    country: int | None = None
    forum: int | None = None
    parent: int | None = None


class DependencyCache:
    def __init__(self):
        self.entries = {}
        self.watchers = defaultdict(set)
        self.dirty = set()
        self.active = None
        self.stats = Counter()
        self.seconds = Counter()

    def watch(self, topic, key):
        if self.active is not None:
            self.active.add((topic, key))

    def changed(self, topic, key):
        self.dirty.update(self.watchers.get((topic, key), ()))

    def evaluate(self, operation, request, compute):
        key = operation, request
        entry = self.entries.get(key)
        if entry is not None and key not in self.dirty:
            self.stats[f"{operation}_reused"] += 1
            return entry[0]
        self.active = set()
        start = perf_counter()
        try:
            result = compute()
            dependencies = self.active
        finally:
            self.active = None
        if entry is not None:
            for token in entry[1] - dependencies:
                self.watchers[token].remove(key)
        for token in dependencies:
            self.watchers[token].add(key)
        self.entries[key] = result, dependencies
        self.dirty.discard(key)
        self.stats[f"{operation}_computed"] += 1
        self.seconds[operation] += perf_counter() - start
        return result


class InteractiveModel:
    def __init__(self):
        self.loading = True
        self.nodes = defaultdict(dict)
        self.messages = {}
        self.ranking = NeighborhoodTopK()
        self.cache = DependencyCache()
        self.friend_dates = defaultdict(dict)
        self.person_city = {}
        self.place_parent = {}
        self.org_place = {}
        self.study = defaultdict(list)
        self.work = defaultdict(list)
        self.interest = defaultdict(set)
        self.interest_scores = defaultdict(dict)
        self.forum_moderator = {}
        self.forums = ForumActivity(self.ranking.adjacent)
        self.by_tag = defaultdict(list)
        self.tags = defaultdict(set)
        self.tag_members = defaultdict(set)
        self.tag_subclasses = defaultdict(set)
        self.posts = defaultdict(list)
        self.comments = defaultdict(list)
        self.by_country = defaultdict(list)
        self.replies = defaultdict(list)
        self.received = defaultdict(list)
        self.reply_weights = Counter()
        self.pending_likes = []
        self.liked = defaultdict(dict)
        self.pending_messages = []
        self.resolved_posts = {}
        self.birthday = {}
        self.first_names = defaultdict(set)
        self.categories = {}
        self.comment_categories = defaultdict(set)

    def add_node(self, label, ident, p):
        if label in ("Post", "Comment"):
            self.messages[ident] = Message(
                p["creationDate"],
                p.get("content") or p.get("imageFile"),
                label == "Post",
            )
            if not self.loading:
                self.pending_messages.append(ident)
        else:
            self.nodes[label][ident] = p
            if label == "Person":
                birth = datetime.fromtimestamp(p["birthday"] / 1000, timezone.utc)
                self.birthday[ident] = birth.month, birth.day
                first = p["firstName"]
                self.first_names[first].add(ident)
                if not self.loading:
                    self.cache.changed("first_name", first)

    def add_edge(self, label, at, a, bt, b, props):
        if label == "HAS_CREATOR":
            self.messages[a].author = b
        elif label == "IS_LOCATED_IN":
            if at == "Person":
                self.person_city[a] = b
            elif at == "Organisation":
                self.org_place[a] = b
            else:
                self.messages[a].country = b
        elif label == "REPLY_OF":
            self.messages[a].parent = b
        elif label == "KNOWS":
            when = props["creationDate"]
            self.friend_dates[a][b] = when
            self.friend_dates[b][a] = when
            appended = b not in self.ranking.adjacent[a]
            self.ranking.add_link(a, b)
            if not self.loading:
                if appended:
                    self.forums.link_added(a, b)
                self.cache.changed("knows", a)
                self.cache.changed("knows", b)
        elif label == "IS_PART_OF":
            self.place_parent[a] = b
        elif label == "STUDY_AT":
            self.study[a].append((b, props["classYear"]))
        elif label == "WORK_AT":
            self.work[a].append((b, props["workFrom"]))
        elif label == "HAS_INTEREST":
            self.interest[a].add(b)
        elif label == "HAS_MODERATOR":
            self.forum_moderator[a] = b
        elif label == "HAS_MEMBER":
            self.forums.add_member(b, a, props["joinDate"])
        elif label == "CONTAINER_OF":
            self.messages[b].forum = a
        elif label == "HAS_TAG" and at == "Post":
            if b not in self.tags[a]:
                self.tags[a].add(b)
                self.by_tag[b].append(a)
                if not self.loading:
                    self.cache.changed("tag", b)
        elif label == "HAS_TAG" and at in ("Forum", "Comment"):
            pass
        elif label == "HAS_TYPE":
            self.tag_members[b].add(a)
        elif label == "IS_SUBCLASS_OF":
            self.tag_subclasses[b].add(a)
        elif label == "LIKES":
            when = props["creationDate"]
            if self.loading:
                self.pending_likes.append((a, b, when))
            else:
                self.add_like(a, b, when)
        else:
            raise ValueError(f"Unmapped graph edge {label} {at}->{bt}")

    def add_like(self, person, message, time):
        owner = self.messages[message].author
        rank = time, -message
        likes = self.liked[owner]
        previous = likes.get(person)
        if previous is None or previous < rank:
            likes[person] = rank
            if not self.loading:
                self.cache.changed("likes", owner)

    def add_message_indexes(self, ident):
        m = self.messages[ident]
        author = m.author
        if author not in self.nodes["Person"] or m.country not in self.nodes["Place"]:
            raise ValueError(f"Message {ident} lacks an author or country")
        rank = (m.when, -ident)
        self.ranking.add_message(author, ident, m.when, m.body)
        insort(self.by_country[author, m.country], m.when)
        self.cache.changed("messages", author)
        self.cache.changed("country", (author, m.country))
        if m.post:
            self.forums.add_post(author, m.forum)
            insort(self.posts[author], rank)
            tags = self.tags.get(ident, ())
            scores = self.interest_scores.get(author, {})
            for seed in scores:
                scores[seed] += -1 if self.interest[seed].isdisjoint(tags) else 1
            self.cache.changed("posts", author)
        else:
            parent = self.messages[m.parent]
            self.comments[author].append(ident)
            self.replies[m.parent].append(ident)
            insort(self.received[parent.author], rank)
            self.cache.changed("replies", m.parent)
            self.cache.changed("received", parent.author)
            if parent.post:
                post_tags = self.tags.get(m.parent, ())
                for category in self.comment_categories.get(author, ()):
                    if not self.categories[category].isdisjoint(post_tags):
                        self.cache.changed("comment_category", (author, category))
            pair = pair_key(author, parent.author)
            self.reply_weights[pair] += 2 if parent.post else 1
            self.cache.changed("weight", pair)

    def finish_load(self):
        people = self.nodes["Person"]
        places = self.nodes["Place"]
        messages = self.messages
        bodies = self.ranking.messages
        by_author = self.ranking.by_author
        by_country = self.by_country
        posts = self.posts
        comments = self.comments
        replies = self.replies
        received = self.received
        reply_weights = self.reply_weights
        add_forum_post = self.forums.add_post
        for ident, message in messages.items():
            author, country, when = message.author, message.country, message.when
            if author not in people or country not in places:
                raise ValueError(f"Message {ident} lacks an author or country")
            rank = (when, -ident)
            bodies[ident] = message.body
            by_author[author].append(rank)
            by_country[author, country].append(when)
            if message.post:
                add_forum_post(author, message.forum)
                posts[author].append(rank)
            else:
                parent = messages[message.parent]
                comments[author].append(ident)
                replies[message.parent].append(ident)
                received[parent.author].append(rank)
                pair = pair_key(author, parent.author)
                reply_weights[pair] += 2 if parent.post else 1
        for index in (
            posts,
            received,
            by_country,
            by_author,
        ):
            for group in index.values():
                group.sort()
        for person, message, time in self.pending_likes:
            self.add_like(person, message, time)
        self.pending_likes.clear()
        self.tags_by_name = {p["name"]: ident for ident, p in self.nodes["Tag"].items()}
        self.classes_by_name = {
            p["name"]: ident for ident, p in self.nodes["TagClass"].items()
        }
        self.country_by_name = {
            p["name"]: ident
            for ident, p in self.nodes["Place"].items()
            if p["type"] == "country"
        }
        self.loading = False

    def finish_commit(self):
        for ident in self.pending_messages:
            self.add_message_indexes(ident)
        self.pending_messages.clear()

    def neighbors(self, person):
        self.cache.watch("knows", person)
        return self.ranking.adjacent.get(person, ())

    def levels(self, person, depth):
        seen = {person}
        current = {person}
        for _ in range(depth):
            following = set()
            for a in current:
                following.update(self.neighbors(a))
            current = following - seen
            seen.update(current)
            yield current

    def friends(self, person, depth=2):
        result = set()
        for layer in self.levels(person, depth):
            result.update(layer)
        return result

    def profile(self, person):
        p = self.nodes["Person"][person]
        return person, p["firstName"], p["lastName"]

    def original(self, message):
        result = self.resolved_posts.get(message)
        if result is not None:
            return result
        chain = set()
        while not self.messages[message].post:
            if message in chain:
                raise ValueError("Cycle in REPLY_OF")
            chain.add(message)
            message = self.messages[message].parent
        for ident in chain:
            self.resolved_posts[ident] = message
        return message

    def short(self, n, q):
        key = next(iter(q.values()))
        if n == 1:
            p = self.nodes["Person"][key]
            return [
                (
                    p["firstName"],
                    p["lastName"],
                    p["birthday"],
                    p["locationIP"],
                    p["browserUsed"],
                    self.person_city[key],
                    p["gender"],
                    p["creationDate"],
                )
            ]
        if n == 2:
            self.cache.watch("messages", key)
            result = []
            for when, neg in newest(self.ranking.by_author.get(key, ()), q["limit"]):
                ident = -neg
                original = self.original(ident)
                result.append(
                    (
                        ident,
                        self.messages[ident].body,
                        when,
                        original,
                        *self.profile(self.messages[original].author),
                    )
                )
            return result
        if n == 3:
            self.cache.watch("knows", key)
            items = sorted(
                self.friend_dates.get(key, {}).items(), key=lambda x: (-x[1], x[0])
            )
            return [(*self.profile(person), when) for person, when in items]
        m = self.messages[key]
        if n == 4:
            return [(m.body, m.when)]
        if n == 5:
            return [self.profile(m.author)]
        if n == 6:
            f = self.messages[self.original(key)].forum
            return [
                (
                    f,
                    self.nodes["Forum"][f]["title"],
                    *self.profile(self.forum_moderator[f]),
                )
            ]
        if n == 7:
            self.cache.watch("replies", key)
            knowing = self.neighbors(m.author)
            comments = sorted(
                self.replies.get(key, ()),
                key=lambda ident: (
                    -self.messages[ident].when,
                    self.messages[ident].author,
                ),
            )
            return [
                (
                    ident,
                    self.messages[ident].body,
                    self.messages[ident].when,
                    *self.profile(self.messages[ident].author),
                    self.messages[ident].author in knowing,
                )
                for ident in comments
            ]
        raise ValueError(n)

    def complex(self, n, q):
        seed = next(iter(q.values()))
        limit = q.get("limit", 20)
        if n == 1:
            name = q["firstName"]
            self.cache.watch("first_name", name)
            candidates = self.first_names.get(name, set())
            if not candidates or limit == 0:
                return []
            chosen = []
            seen = {seed}
            for distance, layer in enumerate(self.levels(seed, 2), 1):
                seen.update(layer)
                chosen.extend(
                    (distance, self.nodes["Person"][p]["lastName"], p)
                    for p in layer & candidates
                )
                if len(chosen) >= limit or not layer:
                    break
            else:
                # A remaining candidate is three hops away precisely when one
                # of its neighbors belongs to the second BFS layer.
                chosen.extend(
                    (3, self.nodes["Person"][p]["lastName"], p)
                    for p in candidates - seen
                    if not layer.isdisjoint(self.neighbors(p))
                )
            return [
                self.friend_profile(ident, last, distance)
                for distance, last, ident in sorted(chosen)[:limit]
            ]
        if n == 2:
            friends = self.neighbors(seed)
            for friend in friends:
                self.cache.watch("messages", friend)
            return self.message_rows(
                self.ranking.top_k(friends, q["maxDate"] + 1, limit)
            )
        if n == 3:
            countries = [
                self.country_by_name[q[k]] for k in ("countryXName", "countryYName")
            ]
            start = q["startDate"]
            end = start + q["durationDays"] * DAY
            rows = []
            for friend in self.friends(seed):
                if self.place_parent[self.person_city[friend]] in countries:
                    continue
                counts = []
                for country in countries:
                    key = (friend, country)
                    self.cache.watch("country", key)
                    dates = self.by_country.get(key, ())
                    counts.append(bisect_left(dates, end) - bisect_left(dates, start))
                if all(counts):
                    rows.append((*self.profile(friend), *counts, sum(counts)))
            return sorted(rows, key=lambda r: (-r[-1], r[0]))[:limit]
        if n == 4:
            start = q["startDate"]
            end = start + q["durationDays"] * DAY
            prior = set()
            counts = Counter()
            for friend in self.neighbors(seed):
                self.cache.watch("posts", friend)
                for when, ident in self.posts.get(friend, ()):
                    if when >= end:
                        break
                    tags = self.tags.get(-ident, ())
                    if when < start:
                        prior.update(tags)
                    else:
                        counts.update(tags)
            return self.tag_counts(counts, prior, limit)
        if n == 5:
            return [
                (self.nodes["Forum"][forum]["title"], -rank)
                for rank, forum in self.forums.read(seed, q["minDate"], limit)
            ]
        if n == 6:
            friends = self.friends(seed)
            known = self.tags_by_name[q["tagName"]]
            self.cache.watch("tag", known)
            counts = Counter(
                tag
                for ident in self.by_tag.get(known, ())
                if self.messages[ident].author in friends
                for tag in self.tags[ident]
                if tag != known
            )
            return self.tag_counts(counts, (), limit)
        if n == 7:
            known = self.neighbors(seed)
            self.cache.watch("likes", seed)
            likes = sorted(
                self.liked.get(seed, {}).items(), key=lambda x: (-x[1][0], x[0])
            )[:limit]
            return [
                (
                    *self.profile(person),
                    when,
                    -neg,
                    self.messages[-neg].body,
                    (when - self.messages[-neg].when) // 60_000,
                    person not in known,
                )
                for person, (when, neg) in likes
            ]
        if n == 8:
            self.cache.watch("received", seed)
            return [
                (
                    *self.profile(self.messages[-neg].author),
                    when,
                    -neg,
                    self.messages[-neg].body,
                )
                for when, neg in newest(self.received.get(seed, ()), limit)
            ]
        if n == 9:
            return self.message_rows(
                self.ranking.read((seed, q["maxDate"], limit), True)
            )
        if n == 10:
            _, candidates = self.levels(seed, 2)
            month = q["month"]
            rows = []
            interest = self.interest.get(seed, set())
            for person in candidates:
                m, d = self.birthday[person]
                if not ((m == month and d >= 21) or (m == month % 12 + 1 and d < 22)):
                    continue
                self.cache.watch("posts", person)
                scores = self.interest_scores[person]
                score = scores.get(seed)
                if score is None:
                    score = sum(
                        -1 if interest.isdisjoint(self.tags.get(-neg, ())) else 1
                        for _, neg in self.posts.get(person, ())
                    )
                    scores[seed] = score
                p = self.nodes["Person"][person]
                rows.append(
                    (
                        *self.profile(person),
                        score,
                        p["gender"],
                        self.nodes["Place"][self.person_city[person]]["name"],
                    )
                )
            return sorted(rows, key=lambda r: (-r[3], r[0]))[:limit]
        if n == 11:
            country = self.country_by_name[q["countryName"]]
            rows = [
                (*self.profile(friend), self.nodes["Organisation"][org]["name"], year)
                for friend in self.friends(seed)
                for org, year in self.work.get(friend, ())
                if year < q["workFromYear"] and self.org_place[org] == country
            ]
            return sorted(
                sorted(rows, key=lambda row: row[3], reverse=True),
                key=lambda row: (row[4], row[0]),
            )[:limit]
        if n == 12:
            category = q["tagClassName"]
            tags = self.categories.get(category)
            if tags is None:
                tags = self.descendant_tags(category)
                self.categories[category] = tags
            result = []
            for friend in self.neighbors(seed):
                self.comment_categories[friend].add(category)
                self.cache.watch("comment_category", (friend, category))
                matched = set()
                count = 0
                for ident in self.comments.get(friend, ()):
                    parent = self.messages[ident].parent
                    if self.messages[parent].post:
                        common = tags.intersection(self.tags.get(parent, ()))
                        if common:
                            matched.update(common)
                            count += 1
                if count:
                    result.append(
                        (
                            *self.profile(friend),
                            sorted(self.nodes["Tag"][t]["name"] for t in matched),
                            count,
                        )
                    )
            return sorted(result, key=lambda r: (-r[-1], r[0]))[:limit]
        if n in (13, 14):
            a, b = list(q.values())[:2]
            dist, paths = shortest(a, b, self.neighbors, paths=n == 14)
            if n == 13:
                return [(dist,)]
            rows = []
            for path in paths:
                weight = 0
                for u, v in zip(path, path[1:]):
                    key = pair_key(u, v)
                    self.cache.watch("weight", key)
                    weight += self.reply_weights[key]
                rows.append((list(path), weight / 2))
            return sorted(rows, key=lambda row: (-row[1], row[0]))
        raise ValueError(n)

    def friend_profile(self, person, last, distance):
        p = self.nodes["Person"][person]

        def orgs(rows):
            return [
                dict(
                    organizationName=self.nodes["Organisation"][org]["name"],
                    year=year,
                    placeName=self.nodes["Place"][self.org_place[org]]["name"],
                )
                for org, year in rows
            ]

        return (
            person,
            last,
            distance,
            p["birthday"],
            p["creationDate"],
            p["gender"],
            p["browserUsed"],
            p["locationIP"],
            split(p.get("email")),
            split(p.get("language")),
            self.nodes["Place"][self.person_city[person]]["name"],
            orgs(self.study.get(person, ())),
            orgs(self.work.get(person, ())),
        )

    def tag_counts(self, counts, exclude, limit):
        result = [
            (self.nodes["Tag"][tag]["name"], count)
            for tag, count in counts.items()
            if tag not in exclude
        ]
        return sorted(result, key=lambda r: (-r[1], r[0]))[:limit]

    def message_rows(self, ranks):
        return [
            (*self.profile(author), ident, self.messages[ident].body, -neg)
            for neg, ident, author in ranks
        ]

    def descendant_tags(self, name):
        result = set()
        if name in self.tags_by_name:
            result.add(self.tags_by_name[name])
        if name not in self.classes_by_name:
            return result
        seen = set()
        visit = [self.classes_by_name[name]]
        while visit:
            cls = visit.pop()
            if cls not in seen:
                seen.add(cls)
                result.update(self.tag_members.get(cls, ()))
                visit.extend(self.tag_subclasses.get(cls, ()))
        return result


def pair_key(a, b):
    return (a, b) if a <= b else (b, a)


def split(value):
    return value.split(";") if value else []


def newest(sorted_rows, limit):
    if limit == 0:
        return ()
    return reversed(sorted_rows[-limit:])
