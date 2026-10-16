#!/usr/bin/env python3
"""Dopecast: listen to what the catalogue stations actually play.

Run every 3 hours by .github/workflows/listen.yml. Each run:
  1. reads the current track title of every catalogue station (a few KB per station, no audio kept);
  2. appends new tracks to listen/history.jsonl (last 21 days are kept);
  3. looks up the main genre of new artists on Deezer (artist + track, cached in listen/artists.json);
  4. writes listen/report.json: per station, how much of what it plays fits its genre.
build_catalog.py uses the report to push off-genre stations to the end of their genre;
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
KEEP_DAYS = 21
WORKERS = 64
TIMEOUT = 8
LOOKUPS_PER_RUN = 1500
UA = "Dopecast-listen/1.0 (+https://github.com)"
CTX = ssl.create_default_context()

# Deezer genres that count as "on genre" for each Dopecast genre ("Other" is eclectic by design)
FIT = {
    "Hip-Hop": {"Rap/Hip Hop"},
    "Soul & Funk": {"R&B", "Soul & Funk", "Disco", "Blues"},
    "Jazz": {"Jazz", "Blues"},
    "Chicano & Latin": {"Latin Music", "Rap/Hip Hop", "R&B", "Soul & Funk", "Brazilian Music", "Reggae"},
    "Mexican": {"Latin Music"},
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


def main():
    os.makedirs(DIR, exist_ok=True)
    cat = json.load(open(os.path.join(HERE, "catalog.json"), encoding="utf-8"))
    stations = [(g, s["n"], s["u"]) for g in FIT for s in cat["genres"].get(g, [])]
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

    # 4. per-station report
    genre_of_url = {s[2]: s[0] for s in stations}
    plays = collections.defaultdict(list)
    for r in hist:
        if r["url"] in genre_of_url:
            plays[r["url"]].append(artists.get(nk(r["artist"]), "?"))
    report = {}
    for url, gs in plays.items():
        known = [x for x in gs if x != "?"]
        g = genre_of_url[url]
        fit = sum(x in FIT[g] for x in known) / len(known) if known else None
        report[url] = {"genre": g, "tracks": len(gs), "known": len(known),
                       "fit": round(fit, 2) if fit is not None else None,
                       "mix": collections.Counter(known).most_common(3)}
    json.dump({"updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "stations": report},
              open(REPORT, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))
    titled = sum(1 for t in titles if t)
    print(f"stations {len(stations)}  sending titles {titled}  new tracks {added}  "
          f"artists looked up {len(items)} (left {max(0, len(todo) - len(items))})  judged {sum(1 for v in report.values() if v['known'] >= 5)}")


if __name__ == "__main__":
    main()
