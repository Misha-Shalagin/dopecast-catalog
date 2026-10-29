#!/usr/bin/env python3
"""Dopecast: listen to what the catalogue stations actually play.

Run every 3 hours by .github/workflows/listen.yml. Each run:
  1. reads the current track title of every station in listen/pool.json (all working candidates, written by
     build_catalog.py; catalog.json until it exists) - a few KB per station, no audio kept;
  2. appends new tracks to listen/history.jsonl (last 21 days are kept);
  3. looks up the main genre of new artists on Deezer (artist + track, cached in listen/artists.json)
     and their fans and debut year (listen/artist_info.json, most played artists first);
  4. writes listen/report.json: per station, how much of what it plays fits its genre (RULES)
     and how much is modern mainstream (debut 2005+ and 1M+ fans on Deezer).
build_catalog.py keeps only stations proven by the report in every genre except Other;
curation.json (reviewed by hand) removes or moves stations for good.
Standard library only.
"""
import collections
import concurrent.futures as cf
import http.client
import json
import os
import re
import socket
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
DIR = os.path.join(HERE, "listen")
HISTORY = os.path.join(DIR, "history.jsonl")
ARTISTS = os.path.join(DIR, "artists.json")
REPORT = os.path.join(DIR, "report.json")
INFO = os.path.join(DIR, "artist_info.json")
POOL = os.path.join(DIR, "pool.json")
INFO_PER_RUN = 1200
MB_PER_RUN = 700                          # MusicBrainz allows one request a second
POP_SINCE, POP_FANS = 2005, 1_000_000     # "modern mainstream": debuted 2005 or later and 1M+ fans
KEEP_DAYS = 14
WORKERS = 64
TIMEOUT = 8
LOOKUPS_PER_RUN = 1500
UA = "Dopecast-listen/1.0 (+https://github.com)"
CTX = ssl.create_default_context()

# What counts as "on genre" for each Dopecast genre ("Other" is eclectic by design):
#   fit - Deezer album genres;  era - artist debut years (needs artist_info);  pop - modern mainstream is judged
LATIN = {"Latin Music", "Traditional Mexicano", "Banda/Grupero", "Bolero", "Cumbia", "Salsa"}
RULES = {
    "Hip-Hop": {"fit": {"Rap/Hip Hop"}, "pop": True},
    "Soul & Funk": {"fit": {"R&B", "Soul & Funk", "Disco", "Blues"}, "pop": True},
    "Jazz": {"fit": {"Jazz", "Blues"}},
    "Chicano & Latin": {"fit": LATIN | {"Rap/Hip Hop", "R&B", "Soul & Funk", "Brazilian Music", "Reggae"}, "pop": True},
    "Mexican": {"fit": LATIN, "pop": True},
    "70s": {"era": (1950, 1979)},
    "80s": {"era": (1960, 1989)},
    "90s": {"era": (1970, 1999)},
    "Rock 80–90": {"fit": {"Rock", "Alternative", "Metal"}, "era": (1960, 1999)},
    "Rhythm & Blues": {"fit": {"R&B", "Soul & Funk", "Blues", "Disco"}, "pop": True},
    "Metal": {"fit": {"Metal", "Rock"}},
    "Easy Listening": {"fit": {"Pop", "Jazz", "Singer & Songwriter", "Films/Games", "Classical", "Folk", "Country", "Soul & Funk"}},
    "Lounge": {"fit": {"Electro", "Jazz", "Dance", "Soul & Funk", "Alternative", "Pop", "Brazilian Music"}},
}


def nk(s):
    return re.sub(r"[\W_]+", "", (s or "").lower())


# ---------- current title of a stream (ICY metadata) ----------
def read_title_from(stream, metaint, t0):
    """stream: object with read(n). Skip `metaint` audio bytes, then read one metadata block."""
    left = metaint
    while left > 0:
        if time.time() - t0 > TIMEOUT:
            return None
        chunk = stream.read(min(left, 16384))
        if not chunk:
            return None
        left -= len(chunk)
    n = stream.read(1)
    if not n:
        return None
    size = n[0] * 16
    if size == 0:
        return ""
    block = b""
    while len(block) < size:
        chunk = stream.read(size - len(block))
        if not chunk:
            break
        block += chunk
    raw = block.split(b"\0", 1)[0]
    for enc in ("utf-8", "cp1251", "latin-1"):
        try:
            text = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    m = re.search(r"StreamTitle='(.*?)';", text, re.S)
    return m.group(1).strip() if m else ""


class SockReader:
    def __init__(self, sock, buf):
        self.sock, self.buf = sock, buf

    def read(self, n):
        if self.buf:
            out, self.buf = self.buf[:n], self.buf[n:]
            return out
        return self.sock.recv(n)


def title_raw(url, t0, hops=0):
    """Plain-socket path for old Shoutcast servers ("ICY 200 OK") and simple redirects."""
    u = urllib.parse.urlsplit(url)
    port = u.port or (443 if u.scheme == "https" else 80)
    sock = socket.create_connection((u.hostname, port), timeout=TIMEOUT)
    try:
        if u.scheme == "https":
            sock = CTX.wrap_socket(sock, server_hostname=u.hostname)
        path = (u.path or "/") + ("?" + u.query if u.query else "")
        sock.sendall(f"GET {path} HTTP/1.0\r\nHost: {u.hostname}\r\nUser-Agent: {UA}\r\nIcy-MetaData: 1\r\n\r\n".encode())
        data = b""
        while b"\r\n\r\n" not in data and len(data) < 16384:
            chunk = sock.recv(4096)
            if not chunk:
                return None
            data += chunk
        head, _, rest = data.partition(b"\r\n\r\n")
        lines = head.decode("latin-1").split("\r\n")
        status = lines[0]
        h = {}
        for line in lines[1:]:
            k, _, v = line.partition(":")
            h[k.strip().lower()] = v.strip()
        if re.search(r" 30[1278]", status) and h.get("location") and hops < 3:
            return title_raw(urllib.parse.urljoin(url, h["location"]), t0, hops + 1)
        if " 200" not in status:
            return None
        mi = int(h.get("icy-metaint", "0") or 0)
        if mi <= 0:
            return None
        return read_title_from(SockReader(sock, rest), mi, t0)
    finally:
        sock.close()


def current_title(url):
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Icy-MetaData": "1"})
        with urllib.request.urlopen(req, timeout=TIMEOUT, context=CTX) as r:
            mi = int(r.headers.get("icy-metaint", "0") or 0)
            if mi <= 0:
                return None
            return read_title_from(r, mi, t0)
    except http.client.HTTPException:
        try:
            return title_raw(url, t0)
        except Exception:
            return None
    except Exception:
        return None


# ---------- "Artist - Title" ----------
NOISE = re.compile(r"\b(advert|commercial|jingle|station id|unknown|untitled|live stream|on air|news|weather)\b", re.I)


def split_title(t):
    if not t or " - " not in t or NOISE.search(t):
        return None
    artist, title = t.split(" - ", 1)
    artist = re.split(r"\s+(?:feat\.?|ft\.?|featuring|&|x|vs\.?|with)\s+|\s*[,;/]\s+", artist.strip(), flags=re.I)[0].strip(" -\"'")
    if len(artist) < 2 or len(artist) > 60 or not title.strip():
        return None
    return artist, title.strip()


# ---------- Deezer: artist + track -> album genre ----------
_lock, _next = threading.Lock(), [0.0]


def deezer(url):
    with _lock:                                   # ~8 requests a second (Deezer allows 50 per 5 s)
        wait = _next[0] - time.time()
        _next[0] = max(time.time(), _next[0]) + 0.125
    if wait > 0:
        time.sleep(wait)
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    for _ in range(3):
        try:
            with urllib.request.urlopen(req, timeout=15, context=CTX) as r:
                j = json.loads(r.read())
            if isinstance(j, dict) and (j.get("error") or {}).get("code") == 4:
                time.sleep(5)
                continue
            return j
        except Exception:
            time.sleep(2)
    return None          # network trouble: not cached, tried again next run


ALBUM = {}


def genre_of(artist, title):
    title = re.sub(r"\s*[\(\[].*?[\)\]]", "", title).strip()
    j = deezer("https://api.deezer.com/search?limit=5&q=" + urllib.parse.quote(f'artist:"{artist}" track:"{title}"'))
    if j is None:
        return None
    data = j.get("data", [])
    if not data:
        j = deezer("https://api.deezer.com/search?limit=5&q=" + urllib.parse.quote(f"{artist} {title}"))
        if j is None:
            return None
        data = j.get("data", [])
    a = nk(artist)
    for t in data:
        n = nk((t.get("artist") or {}).get("name"))
        if n and len(a) > 2 and (n == a or n in a or a in n):
            alb = (t.get("album") or {}).get("id")
            if not alb:
                return "?"
            if alb not in ALBUM:
                j = deezer(f"https://api.deezer.com/album/{alb}")
                if j is None:
                    return None
                gs = j.get("genres", {}).get("data", [])
                ALBUM[alb] = gs[0]["name"] if gs else "?"
            return ALBUM[alb]
    return "?"


def info_of(artist):
    """[Deezer fans, debut year] of an artist; [0, 0] if not found; None on network trouble."""
    j = deezer("https://api.deezer.com/search/artist?limit=5&q=" + urllib.parse.quote(artist))
    if j is None:
        return None
    a = nk(artist)
    for x in j.get("data", []):
        if nk(x.get("name")) == a:
            al = deezer(f"https://api.deezer.com/artist/{x['id']}/albums?limit=100")
            if al is None:
                return None
            years = [int(d["release_date"][:4]) for d in al.get("data", []) if (d.get("release_date") or "")[:4].isdigit()]
            years = [y for y in years if y > 1900]
            return [x.get("nb_fan") or 0, min(years) if years else 0]
    return [0, 0]


def mb_start(artist):
    """Year the artist started, from MusicBrainz: a band's founding year, a person's birth year + 20.
    Deezer dates come from the albums it has, often remasters (Creedence 2006, John Lennon 2007), so the earlier wins.
    0 if unknown; None on network trouble."""
    url = "https://musicbrainz.org/ws/2/artist/?fmt=json&limit=3&query=" + urllib.parse.quote(f'artist:"{artist}"')
    req = urllib.request.Request(url, headers={"User-Agent": "Dopecast-listen/1.0 (https://github.com/Misha-Shalagin/dopecast-catalog)"})
    for _ in range(2):
        time.sleep(1.1)
        try:
            with urllib.request.urlopen(req, timeout=15, context=CTX) as r:
                j = json.loads(r.read())
            break
        except Exception:
            j = None
    if j is None:
        return None
    for x in j.get("artists", []):
        y = ((x.get("life-span") or {}).get("begin") or "")[:4]
        if x.get("score", 0) >= 95 and nk(x.get("name")) == nk(artist) and y.isdigit():
            return int(y) + (20 if x.get("type") == "Person" else 0)
    return 0


def debut(inf):
    """Earliest known start of an artist: Deezer albums or MusicBrainz, whichever is earlier."""
    ys = [y for y in (inf or [])[1:3] if y]
    return min(ys) if ys else 0


def judge(g, keys, artists, info):
    """Per station: plays judged, share on genre, share modern mainstream (None until 10 plays have artist info)."""
    rule = RULES[g]
    known = on = en = pop = 0
    mix = collections.Counter()
    for k in keys:
        gen, inf = artists.get(k, "?"), info.get(k) or [0, 0]
        d = debut(inf)
        if gen != "?":
            mix[gen] += 1
        if d:
            en += 1
            pop += d >= POP_SINCE and inf[0] >= POP_FANS
        if ("fit" in rule and gen == "?") or ("era" in rule and not d):
            continue
        known += 1
        on += ("fit" not in rule or gen in rule["fit"]) and ("era" not in rule or rule["era"][0] <= d <= rule["era"][1])
    return {"genre": g, "tracks": len(keys), "known": known,
            "fit": round(on / known, 2) if known else None,
            "pop": round(pop / en, 2) if en >= 10 else None,
            "mix": mix.most_common(3)}


def main():
    os.makedirs(DIR, exist_ok=True)
    src = POOL if os.path.exists(POOL) else os.path.join(HERE, "catalog.json")
    cat = json.load(open(src, encoding="utf-8"))
    stations = [(g, s["n"], s["u"]) for g in RULES for s in cat["genres"].get(g, [])]
    now = int(time.time())

    # 1-2. what is playing now -> history
    with cf.ThreadPoolExecutor(WORKERS) as pool:
        titles = list(pool.map(lambda s: current_title(s[2]), stations))
    hist = []
    if os.path.exists(HISTORY):
        for line in open(HISTORY, encoding="utf-8"):
            try:
                r = json.loads(line)
            except Exception:
                continue
            if now - r["ts"] < KEEP_DAYS * 86400:
                hist.append(r)
    last = {}
    for r in hist:
        last[r["url"]] = r["raw"]
    added = 0
    for (g, name, url), t in zip(stations, titles):
        p = split_title(t)
        if not p or last.get(url) == t:
            continue
        hist.append({"ts": now, "url": url, "raw": t, "artist": p[0], "title": p[1]})
        added += 1
    with open(HISTORY, "w", encoding="utf-8") as f:
        for r in hist:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    # 3. genres of new artists
    artists = json.load(open(ARTISTS, encoding="utf-8")) if os.path.exists(ARTISTS) else {}
    todo = {}
    for r in hist:
        k = nk(r["artist"])
        if k and k not in artists and k not in todo:
            todo[k] = (r["artist"], r["title"])
    items = list(todo.items())[:LOOKUPS_PER_RUN]
    with cf.ThreadPoolExecutor(6) as pool:
        for (k, (a, t)), g in zip(items, pool.map(lambda it: genre_of(*it[1]), items)):
            if g is not None:
                artists[k] = g
    json.dump(artists, open(ARTISTS, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))

    # 3b. fans and debut year, most played artists first
    info = json.load(open(INFO, encoding="utf-8")) if os.path.exists(INFO) else {}
    count = collections.Counter(nk(r["artist"]) for r in hist)
    names = {nk(r["artist"]): r["artist"] for r in hist}
    need = [k for k, _ in count.most_common() if k and k not in info and artists.get(k, "?") != "?"][:INFO_PER_RUN]
    with cf.ThreadPoolExecutor(6) as pool:
        for k, v in zip(need, pool.map(lambda k: info_of(names[k]), need)):
            if v is not None:
                info[k] = v
    # 3c. MusicBrainz start year (fixes remaster dates), most played first
    mb = [k for k, _ in count.most_common() if k in info and len(info[k]) < 3 and not 0 < info[k][1] < 1980][:MB_PER_RUN]   # an early Deezer date is already right
    for k in mb:
        y = mb_start(names[k])
        if y is not None:
            info[k] = info[k][:2] + [y]
    json.dump(info, open(INFO, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))

    # 4. per-station report
    genre_of_url = {s[2]: s[0] for s in stations}
    plays = collections.defaultdict(list)
    for r in hist:
        if r["url"] in genre_of_url:
            plays[r["url"]].append(nk(r["artist"]))
    report = {url: judge(genre_of_url[url], keys, artists, info) for url, keys in plays.items()}
    json.dump({"updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "stations": report},
              open(REPORT, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))
    titled = sum(1 for t in titles if t)
    print(f"stations {len(stations)}  sending titles {titled}  new tracks {added}  "
          f"artists looked up {len(items)} (left {max(0, len(todo) - len(items))})  artist info {len(need)} MusicBrainz {len(mb)}  judged {sum(1 for v in report.values() if v['known'] >= 5)}")


if __name__ == "__main__":
    main()
