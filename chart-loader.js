(function () {
    // Loads the charting libraries, which are vendored under lib/ and served
    // from this origin rather than a CDN. Nothing here reaches a third party.
    //
    // The directory is "lib" rather than "vendor" deliberately: some shared
    // hosts block any path containing /vendor/ outright, since it is a known
    // RCE vector (/vendor/phpunit/...), and every file under it 403s.
    //
    // No subresource integrity. Integrity hashes guard against a CDN serving
    // different bytes than you expect; for a file on your own origin, the same
    // trust that covers the page covers the script.
    //
    // Loaded from a separate file rather than an inline <script> so the page
    // carries no inline script and serves unchanged from a host with a strict
    // policy against it.
    function loadScript(src) {
        return new Promise(function (resolve, reject) {
            var s = document.createElement('script');
            s.src = src;
            s.onload = resolve;
            s.onerror = reject;
            document.body.appendChild(s);
        });
    }

    // Kept for genuine failures — a missing file, a truncated deploy. The note
    // no longer mentions privacy settings, because they are no longer why a
    // chart would be absent.
    function markChartsUnavailable() {
        document.documentElement.classList.add('charts-unavailable');
        function annotate() {
            document.querySelectorAll('canvas').forEach(function (canvas) {
                var wrap = canvas.closest('.chart-wrap') || canvas.parentElement;
                if (!wrap || wrap.querySelector('.chart-unavailable-note')) {
                    return;
                }
                var note = document.createElement('p');
                note.className = 'chart-unavailable-note';
                note.textContent = 'Interactive charts could not be loaded. Tabular data on this page remains available.';
                canvas.style.display = 'none';
                wrap.appendChild(note);
            });
        }
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', annotate);
        } else {
            annotate();
        }
    }

    window.loadChartScripts = function (urls, dashboardSrc) {
        urls.reduce(function (chain, url) {
            return chain.then(function () { return loadScript(url); });
        }, Promise.resolve())
            .then(function () { return dashboardSrc ? loadScript(dashboardSrc) : undefined; })
            .catch(markChartsUnavailable);
    };

    var tag = document.currentScript;
    if (tag && tag.dataset.deps) {
        var urls = tag.dataset.deps.split('|').map(function (s) { return s.trim(); }).filter(Boolean);
        var app = tag.dataset.app || 'dashboard.js';
        loadChartScripts(urls, app);
    }
})();
