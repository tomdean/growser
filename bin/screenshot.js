var args = require('system').args;
var url = args[1];
var destination = args[2];

if (args.length < 2) {
    console.log('Usage: phantomjs screenshot.js URL DESTINATION')
    phantom.exit(1);
}

var resourceWait  = 500,
    maxRenderWait = 15000;

var page = require('webpage').create(),
    count = 0,
    forcedRenderTimeout,
    renderTimeout;

page.viewportSize = {
  width: 1024,
  height: 1536
};

function render_screenshot() {
    page.render(destination);
    phantom.exit();
}

page.onResourceRequested = function (req) {
    count += 1;
    clearTimeout(renderTimeout);
};

page.onResourceReceived = function (res) {
    if (!res.stage || res.stage === 'end') {
        count -= 1;
        if (count === 0) {
            renderTimeout = setTimeout(render_screenshot, resourceWait);
        }
    }
};

page.open(url, function (status) {
    if (status !== "success") {
        console.log('Unable to load url');
        phantom.exit();
    } else {
        forcedRenderTimeout = setTimeout(function () {
            render_screenshot();
        }, maxRenderWait);
    }
});