const CACHE_NAME = "shisha-stock-v1";

const urlsToCache = [
    "/",
    "/static/style.css",
    "/static/manifest.json",
    "/static/icon-192.png",
    "/static/icon-512.png"
];

self.addEventListener("install", (event) => {

    event.waitUntil(

        caches.open(CACHE_NAME)
            .then((cache) => {
                return cache.addAll(urlsToCache);
            })

    );

});


self.addEventListener("fetch", (event) => {

    event.respondWith(

        caches.match(event.request)
            .then((response) => {

                if (response) {
                    return response;
                }

                return fetch(event.request);

            })

    );

});


self.addEventListener("activate", (event) => {

    event.waitUntil(

        caches.keys().then((cacheNames) => {

            return Promise.all(

                cacheNames.map((cache) => {

                    if (cache !== CACHE_NAME) {
                        return caches.delete(cache);
                    }

                })

            );

        })

    );

});