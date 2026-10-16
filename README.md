# Dopecast station catalogue

Every day GitHub opens each candidate stream like the app does (it must answer and actually send audio),
notes codec, bitrate and whether it announces track titles, and publishes `catalog.json`, best stations first.

The Dopecast app downloads `catalog.json` from this repository at start and falls back to its built-in list offline.

* Run now: **Actions → Station catalogue → Run workflow**.
* Sources: radio-browser.info directory and the open playlists of github.com/junguler/m3u-radio-music-playlists.
