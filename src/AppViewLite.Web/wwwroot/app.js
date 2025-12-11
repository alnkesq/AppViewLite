

var pageLoadedTimeBeforeInitialScroll = Date.now();
var visualViewportWasResizedSinceLastTheaterOrMenuOpen = false;
var liveUpdatesPostIds = new Set();
var pageTitleOverride = null;
var notificationCount = parseInt(document.querySelector('.sidebar .notification-badge')?.textContent ?? '0');
var conversationCount = parseInt(document.querySelector('.sidebar .chat-badge')?.textContent ?? '0');


var currentFeedHasNewPosts = false;
var currentFeedHasNewPostsDelay = -1;
var currentFeedHasNewPostsTimeout = null;

var theaterReturnUrl = null;

var previousTabbedListHeaderScrollX = 0;
var historyStack = [];
var applyFocusOnNextPopstate = false;
var isNoLayout = !document.querySelector('.page')

/** @type {Map<string, WeakRef<HTMLElement>>} */
var pendingProfileLoads = new Map();
/** @type {Map<string, WeakRef<HTMLElement>>} */
var pendingPostLoads = new Map();


function updateBadge(selector, count) { 
    var badge = document.querySelector(selector);
    if (badge) {
        badge.textContent = count;
        badge.classList.toggle('display-none', count == 0);
    }
}

function updatePageTitle() {
    var notificationOrConvoCount = notificationCount + conversationCount;
    document.title = (notificationOrConvoCount ? '(' + notificationOrConvoCount + ') ' : '') + (pageTitleOverride ?? appliedPageObj.title);
    updateBadge('.sidebar .notification-badge', notificationCount);
    updateBadge('.bottom-bar .notification-badge', notificationCount);
    updateBadge('.sidebar .chat-badge', conversationCount);
    updateBadge('.bottom-bar .chat-badge', conversationCount);
}


function parseHtmlAsElement(html) { 
    return parseHtmlAsWrapper(html).firstElementChild;
}
function parseHtmlAsWrapper(html) { 
    var temp = document.createElement('div');
    temp.innerHTML = html;
    return temp;
}

function getPostSelector(did, rkey) { 
    return '.post[data-postrkey="' + rkey + '"][data-postdid="' + did + '"]'
}

var recentHandleVerifications = new Map();

function updateHandlesForDid(did) { 
    var handle = recentHandleVerifications.get(did);
    if (handle === undefined) return;
    for (const a of document.querySelectorAll('.handle-generic[data-handledid="' + did + '"]')) {
        a.textContent = handle ?? 'handle.invalid';
        a.classList.remove('handle-uncertain');
        a.classList.toggle('handle-invalid', !handle);
    }
    if (handle) {
        for (const a of document.querySelectorAll("a[href*='/@" + did + "']")) { 
            a.href = replaceDidUrlWithHandle(a.href, did, handle);
        }
        for (const a of document.querySelectorAll("a[data-theaterurl*='/@" + did + "']")) { 
            a.dataset.theaterurl = replaceDidUrlWithHandle(a.dataset.theaterurl, did, handle);
        }
        for (const a of document.querySelectorAll('.profile-badge-pending[data-badgedid="' + did + '"][data-badgehandle="' + handle.toLowerCase() + '"]')) {
            a.classList.remove('profile-badge-pending');
        }
    }
}

function replaceDidUrlWithHandle(href, did, handle) { 
    var url = href.startsWith('/') ? new URL(window.location.origin + href) : new URL(href);
    if (url.origin == location.origin) { 
        var segments = url.pathname.split('/');
        if (segments[1] == '@' + did) { 
            return '/@' + handle + url.pathname.substring(did.length + 2) + url.search;
        }
    }
    return href;
}



class CustomSignalrReconnectPolicy {
    constructor() {
        this._retryDelays = [0, 2000, 5000, 10000, 20000];
    }
    nextRetryDelayInMilliseconds(retryContext) {
        var r = this._retryDelays[retryContext.previousRetryCount];
        if (r === null) return 30000;
        return r;
    }
}

function getDisplayTextForException(error) { 
    if (error instanceof Error) {
        if (error.message == 'NetworkError when attempting to fetch resource.' || error.message == 'Failed to fetch')
            return 'Could not connect to the AppViewLite server.'
        return error.message;
    }
    return '' + error;
}

var liveUpdatesConnection = null;
var liveUpdatesConnectionFuture = (async () => {


    var connection = new signalR.HubConnectionBuilder()
        .withUrl("/api/live-updates?activedid=" + encodeURIComponent(document.documentElement.dataset.currentlyactivedid || ''))
        .configureLogging(signalR.LogLevel.Information) 
        .withAutomaticReconnect(new CustomSignalrReconnectPolicy())
        .build();
    connection.on('PostEngagementChanged', (stats, ownRelationship) => {
        //console.log('PostEngagementChanged: ');
        for (const postElement of document.querySelectorAll(getPostSelector(stats.did, stats.rKey))) {

            var likeToggler = getOrCreateLikeToggler(stats.did, stats.rKey, postElement);
            likeToggler.applyLiveUpdate(stats.likeCount, ownRelationship?.likeRkey);

            var bookmarkToggler = getOrCreateBookmarkToggler(stats.did, stats.rKey, postElement);
            bookmarkToggler.applyLiveUpdate(0, ownRelationship?.bookmarkRkey);

            postElement.dataset.quotecount = stats.quoteCount;

            var repostToggler = getOrCreateRepostToggler(stats.did, stats.rKey, postElement);
            repostToggler.applyLiveUpdate(stats.repostCount, ownRelationship?.repostRkey);
            
            setPostStats(postElement, stats.replyCount, 'replies', 'reply', 'replies');
            intersectionObserver.observe(postElement);
        }
    });
    connection.on('NotificationCount', (notifCount, convoCount) => {
        notificationCount = notifCount;
        conversationCount = convoCount;
        updatePageTitle();
    });
    connection.on('ProfileRendered', (nodeid, html) => { 
        var oldnoderef = pendingProfileLoads.get(nodeid);
        var oldnode = oldnoderef?.deref();
        if (!oldnode) { 
            if (oldnoderef) pendingProfileLoads.delete(nodeid);
            return;
        }
        var newnode = parseHtmlAsElement(html);
        oldnode.replaceWith(newnode);
        updateHandlesForDid(newnode.dataset.profiledid);
    });
    connection.on('PostRendered', (nodeid, html) => { 
        var oldnoderef = pendingPostLoads.get(nodeid);
        var oldnode = oldnoderef?.deref();
        if (!oldnode) { 
            if (oldnoderef) pendingPostLoads.delete(nodeid);
            return;
        }
        var newnode = parseHtmlAsElement(html);
        newnode.didAppearInViewport = oldnode.didAppearInViewport;
        oldnode.replaceWith(newnode);
        //if (newnode.querySelector(".post-quoted[data-pendingload='1']"))

        updateHandlesForDid(newnode.dataset.postdid);
        updateLiveSubscriptions();
    });
    connection.on('HandleVerificationResult', (did, handle) => {
        recentHandleVerifications.set(did, handle);
        updateHandlesForDid(did);
        updateSearchAutoComplete();
    });


    connection.on('SearchAutoCompleteProfileDetails', () => {
        updateSearchAutoComplete();

    });

    if (!isNoLayout) { 
        await connectSignalrIfNecessary(connection);

        async function scheduleReconnect() { 
            setTimeout(async () => {
                // retry policy alone doesn't always work
                await connectSignalrIfNecessary(connection);
                scheduleReconnect();
            }, 30000);
        }
        
        scheduleReconnect();
    }

    liveUpdatesConnection = connection;
    return connection;
})();

var lastAutocompleteRefreshToken = 0;

function updateSearchAutoComplete() { 
    var token = ++lastAutocompleteRefreshToken;
    setTimeout(() => { 
        if (token != lastAutocompleteRefreshToken) return;
        var menu = document.querySelector('.search-form-suggestions');
        if (menu && !menu.classList.contains('display-none')) {
            searchBoxAutocomplete(true)
        }
    }, 200)
}

async function connectSignalrIfNecessary(connection) {
    if (connection.state != 'Disconnected') return;

    var retryCount = 0;
    var retryPolicy = new CustomSignalrReconnectPolicy();
    while (true) {
        if (connection.state == 'Connected') return;
        try {
            await connection.start();
            break;
        } catch (e) { 
            console.error(getDisplayTextForException(e));
        }
        var delayMs = retryPolicy.nextRetryDelayInMilliseconds({ previousRetryCount: retryCount })
        await new Promise(resolve => setTimeout(resolve, delayMs));
        retryCount++;
    }
}

function applyPageFocus() {
    
    var autofocus = document.querySelector('[autofocus]');
    var focalPost = document.querySelector('.post-focal');

    if (autofocus) {
        var prev = document.scrollingElement.scrollTop;
        
        if (autofocus.classList.contains('compose-textarea'))
            autofocus.setSelectionRange(autofocus.value.length, autofocus.value.length);
        autofocus.focus();
        var autofocusPosition = autofocus.getBoundingClientRect();
        if (autofocusPosition.top < 0 || autofocusPosition.bottom > getViewportHeight()) {
            autofocus.scrollIntoView();
            if (prev != document.scrollingElement.scrollTop)
                pageLoadedTimeBeforeInitialScroll = null;
        }
    }
    else if (focalPost && document.querySelector('.post') != focalPost || tryTrimMediaSegments(location.href)) {
        // HACK: with theater, scroll to first post instead of (0,0) so that scroll up closes the theater (otherwise there would be no event)
        pageLoadedTimeBeforeInitialScroll = null;
        focalPost.scrollIntoView();
    }
    else {
        
        window.scrollTo({ top: 0, left: 0, behavior: 'instant' });
    }

    for (const video of document.querySelectorAll('video[autoplay]')) {
        if (!video.didAutoPlay) {
            video.didAutoPlay = true;
            video.play();
        }
    }

    var newestNotificationIdElement = document.querySelector('#notification-newest-id');
    var seenNotificationId = newestNotificationIdElement?.dataset['newestnotification'];
    if (seenNotificationId) {
        var token = applyPageId;
        setTimeout(() => {
            if (applyPageId != token) return;
            document.querySelectorAll('.notification-new').forEach(x => x.classList.remove('notification-new'));
            
            if (+newestNotificationIdElement.dataset.dark == 0)
                notificationCount = 0;
            updatePageTitle();
            httpPost('MarkLastSeenNotification', {
                notificationId: seenNotificationId
            });
        }, 700);
    }
    

    var searchbox = document.querySelector('.search-form-query input');
    var query = null;
    if (searchbox) { 
        var query = searchbox.value;
        searchbox.setSelectionRange(query.length, query.length);
    }

    
}

function closeTheater() { 
    if (document.querySelector('.theater').classList.contains('display-none')) return;
    fastNavigateTo(theaterReturnUrl ?? trimMediaSegments(location.href), false, false);
}

function updateBottomBarSelectedTab() { 
    var url = new URL(window.location.href);
    for (const a of document.querySelectorAll('.bottom-bar a')) {
        var path = new URL(a.href).pathname
        var selected = path == url.pathname
        a.firstElementChild.classList.toggle('display-none', selected)
        a.firstElementChild.nextElementSibling.classList.toggle('display-none', !selected)
    }
}

var userSelectedTextSinceLastMouseDown = false;

document.addEventListener("selectionchange", () => {
    const selection = window.getSelection().toString();
    if (selection.length > 0) {
        userSelectedTextSinceLastMouseDown = true;
    }
});


function fastNavigateIfLink(event) { 
    var url = null;
    var t = event.target;
    var a = null;

    if (t instanceof HTMLElement) { 
        var postBody = t.closest('.post-body');
        if (postBody && !postBody.parentElement.classList.contains('post-focal') && !t.closest('a')) {
            var backgroundLink = getPostPreferredUrlElement(postBody.parentElement);
            if (!userSelectedTextSinceLastMouseDown) {
                recordPostEngagement(backgroundLink.closest('.post'), 'OpenedThread');
                if (backgroundLink.target == '_blank') window.open(backgroundLink.href);
                else fastNavigateTo(backgroundLink.href);
                return true;
            }
        }
    }

    while (t) {
        if (t.tagName == 'A' && t.href) {
            a = t;
            url = new URL(t.href);
            break;
        }
        t = t.parentNode;
    }


    if (!url) return false;

    if (pageAutoRefreshTimeout !== null) { 
        clearTimeout(pageAutoRefreshTimeout);
        pageAutoRefreshTimeout = null;
    }

    if (a.parentElement?.classList?.contains('pagination-button')) { 
        event.preventDefault();
        loadNextPage(true);
        return true;
    }

    var theaterUrl = a.dataset.theaterurl;
    if (a.classList.contains('post-image-for-threater')) { 
        theaterReturnUrl = location.href;
        recordPostEngagement(a.closest('.post'), 'ViewedInTheaterOrWatchedVideo'); // So that we preserve SeenInFollowingFeed flag (page URL hasn't changed yet)
        fastNavigateTo(theaterUrl, false, false);
        event.preventDefault();
        return true;
    }

    if (a.classList.contains('image-grid-cell-link')) { 
        theaterReturnUrl = location.href;
    }

    if (a.id == 'bottom-bar-home-button') { 
        var previousPage = historyStack[historyStack.length - 1];
        if (previousPage == url) {
            console.log('Bottom bar home button: Back');
            history.back();

            event.preventDefault();
            return true;
        }
        
        if (url != location.href) {
            console.log('Navigate to home, avoid refresh')
            fastNavigateTo(url.href, false, false);

            event.preventDefault();
            return true;
        }
    }

    if (a.classList.contains('blue-link') && a.closest('.post-body')) { 
        recordPostEngagement(a.closest('.post'), 'OpenedExternalLink');
    }

    if (a.classList.contains('post-external-preview')) { 
        recordPostEngagement(a.closest('.post'), 'OpenedExternalLink');
    }

    if (a.classList.contains('media-download-menu-item')) { 
        recordPostEngagement(a.closest('.post'), 'Downloaded');
    }

    if (a.classList.contains('post-background-link')) { 
        recordPostEngagement(a.closest('.post'), 'OpenedThread');
    }

    if (a.target || a.download)
        return false;

    if (canFastNavigateTo(url)) {
        const preferRefresh = NO_FETCH_REUSE_PATHS.includes(url.pathname) || a.closest('.notification') ? true : null;
        fastNavigateTo(url.href, preferRefresh, a.dataset.alwaysfocuspage == '1' ? true : null);
        event.preventDefault();
        return true;
    }
    return false;
}

var NO_FETCH_REUSE_PATHS = ['/notifications', '/history', '/debug/event-charts'];
var NO_FAST_NAVIGATE_PATHS = ['/login', '/logout', '/set-active-profile', '/settings/mute', '/debug/locks', '/debug/requests', '/debug/log', '/debug/performance'];

function canFastNavigateTo(url) { 
    if (url.host != window.location.host) return false;
    if (NO_FAST_NAVIGATE_PATHS.includes(url.pathname)) return false;
    if (url.pathname.startsWith('/img/') || url.pathname.startsWith('/watch/') || url.pathname.startsWith('/api/')) return false;
    return true;
}

/**@type {href: string, dateFetched: number, dom: HTMLElement, title: string, scrollTop: number}[] */
var recentPages = [];
var applyPageId = 0;

var appliedPageObj = null;

async function applyPage(href, preferRefresh = null, scrollToTop = null) { 
    console.log('Load: ' + href);
    var token = ++applyPageId;
    
    preferRefresh = preferRefresh ?? (appliedPageObj.href == href);
    scrollToTop ??= true;

    if (preferRefresh) { 
        recentPages = recentPages.filter(x => x.href != href);
    }
    updateBottomBarSelectedTab();

    var oldMain = document.querySelector('main');


    let restoreSamePageScroll = null;

    previousTabbedListHeaderScrollX = document.querySelector('.tabbed-lists-header-inner')?.scrollLeft ?? 0;
    var theaterForPostInfo = tryTrimMediaSegments(location.href);
    var theaterForPostElement = theaterForPostInfo?.getPostElement();
    if (theaterForPostElement) {
        var body = getPostText(theaterForPostElement);
        pageTitleOverride = getTextIncludingEmojis(theaterForPostElement.querySelector('.post-author')) + (body ? ': ' + body : '');
    }else{
        
        var oldMainWasHidden = false;
        setTimeout(() => {
            if (token != applyPageId) return;
            if (oldMainWasHidden) return;

            oldMainWasHidden = true;
            appliedPageObj.scrollTop = document.scrollingElement.scrollTop;

            var spinnerMain = document.createElement('main');
            spinnerMain.innerHTML = '<div class="pagination-button spinner-visible whole-page-spinner">' + SPINNER_HTML + '</div>'
            oldMain.replaceWith(spinnerMain);
            oldMain = spinnerMain;
        }, 1000);

        try {
            var p = await fetchOrReusePageAsync(href, token);
        } catch (e) {
            if (token == applyPageId) {
                location.href = href;
                return;
            }
            throw e;
        }
        if (token != applyPageId) throw 'Superseded navigation.'
    
        if (p.href != href && !theaterForPostInfo) { 
            href = p.href;
            // redirection
            history.replaceState(null, null, href);
        }

        if (appliedPageObj.href == href)
            restoreSamePageScroll = document.scrollingElement.scrollTop;

        p.dom.classList.remove('display-none');
        var page = oldMain.parentElement;
    
        if (!oldMainWasHidden)
            appliedPageObj.scrollTop = document.scrollingElement.scrollTop;

        if (p.dom != oldMain) {
            oldMain.remove();
            page.appendChild(p.dom);
        }
    
        oldMainWasHidden = true;

        appliedPageObj = p;
        pageTitleOverride = null;

    }

    applyPageElements();

    if (scrollToTop) {
        applyPageFocus();
    } else if(p) { 
       document.scrollingElement.scrollTop = restoreSamePageScroll ?? p.scrollTop;
    }
    prevScrollTop = 0;
    onPageScrollPositionFinalized();

    updateSidebarButtonScrollVisibility();
}

function getTextIncludingEmojis(node) { 
    if (!node) return '';
    if (node instanceof Element) {

        if (node.tagName == 'IMG') {
            return node.alt || '';
        }

        var sb = '';
    
        for (const child of node.childNodes) {
            sb += getTextIncludingEmojis(child);
        }

        if (node.tagName == 'A' && (sb.startsWith('https://') || sb.startsWith('http://'))) {
            return node.href;
        }
        return sb;
    } else { 
        return node.textContent;
    }
}


async function safeSignalrInvoke(methodName, ...args) { 
    const start = Date.now();
    var didLogErrorOnce = false;
    while (true) { 
        try {
            await Promise.race([liveUpdatesConnectionFuture, await new Promise(resolve => setTimeout(resolve, 3000))]);
            var connection = liveUpdatesConnection
            if (connection == null) throw 'The initial socket connection has not completed yet.'
            var result = await connection.invoke(methodName, ...args);
            if (didLogErrorOnce) { 
                console.log('SignalR invocation reattempt of ' + methodName + ' now succeeded after ' + (Date.now() - start) / 1000 + ' seconds');
            }
            return result;
        } catch (e) { 
            if (connection && connection.state == 'Connected') throw e; // User code error, not a websocket problem
            if (Date.now() - start > 300000) {
                throw new Error('SignalR invocation reattempts have not succeeded yet for ' + methodName + ', giving up: ' + getDisplayTextForException(e));
            }
            if (!didLogErrorOnce)
                console.error('SignalR invocation of ' + methodName + ' failed because the socket is not connected. Will keep retrying. ' + getDisplayTextForException(e))
            didLogErrorOnce = true;
            await new Promise(resolve => setTimeout(resolve, 1000));
        }
    }
    
}

async function recordPostEngagement(postElement, kind) {
    var quoterPost = postElement.parentElement?.closest('.post'); // post might be detached from DOM if postElement was set before navigation to thread page (and postElement comes from .theater-image -> postElement)
    if (quoterPost)
        recordPostEngagement(quoterPost, kind);

    var kinds = kind.split(',');

    // if (postElement.classList.contains('post-blocked')) return;

    if (location.pathname == '/following' && !kinds.includes('SeenInFollowingFeed')) { 
        kinds.push('SeenInFollowingFeed');
    }


    kinds = kinds.filter(x => !postElement['recordedEngagement_' + x]);
    if (kinds.length == 0) return;
    for (const x of kinds) {
        postElement['recordedEngagement_' + x] = true;
    }

    kind = kinds.join(',');
    console.log('Engagement: ' + kind + ' for /@' + postElement.dataset.postdid + '/' + postElement.dataset.postrkey);
    var hasImages = !!postElement.querySelector('.post-image-list') && !postElement.classList.contains('post-small-media-thumbnail');
    var hasExternalPreview = !!postElement.querySelector('.post-external-preview-image') && !postElement.querySelector('.post-external-preview-compact');
    var weight = hasImages || hasExternalPreview ? 2 : 1;
    safeSignalrInvoke('MarkAsRead', postElement.dataset.postdid, postElement.dataset.postrkey, postElement.dataset.fromfeeddid, postElement.dataset.fromfeedrkey, kind, weight);
    postElement.wasMarkedAsRead = true;
}

var intersectionObserver;

var pageAutoRefreshTimeout = null;

function applyPageElements() { 
    

    currentFeedHasNewPosts = false;
    document.querySelector('.scroll-up-button-badge').classList.add('display-none');
    clearFeedUpdateCheckTimeout();

    updatePageTitle();
    updateLiveSubscriptions();
    updateSidebarButtonScrollVisibility();
    updateBottomBarSelectedTab();
    composeTextAreaChanged();
    
    var theaterInfo = tryTrimMediaSegments(location.href);
    var isTheater = !!theaterInfo;

    var theaterBackground = document.body.querySelector('.theater-background');
    theaterBackground.classList.toggle('display-none', !isTheater)

    var theater = document.body.querySelector('.theater');
    theater.classList.toggle('display-none', !isTheater)
    if (isTheater) {
        
        var includePostText = theaterReturnUrl && (theaterReturnUrl.includes('?media=1') || theaterReturnUrl.includes('kind=media'));
        var a = theaterInfo.getImageLinkElement();
        
        var postElement = a.closest('.post');
        var postText = includePostText ? getPostText(postElement) : null;
        
        document.querySelector('.theater-image').src = ''; // ensure old image is never displayed
        document.querySelector('.theater-image').src = a.href;
        document.querySelector('.theater-image').postElement = postElement;
        var alt = a.title;
        var description = alt && postText ? postText + "\n\nImage description:\n" + alt : (alt || postText);

        var descriptionElement = document.querySelector('.theater-alt');
        descriptionElement.textContent = description;
        descriptionElement.classList.toggle('display-none', !description);
        descriptionElement.classList.toggle('theater-alt-reduced-max-height', includePostText);
        emojify(descriptionElement);
        document.querySelector('.theater-date').textContent = ' · ' + postElement.dataset.displayshortdate;
        document.querySelector('.theater-full-post-link').classList.toggle('display-none', !includePostText);
        document.querySelector('.theater-full-post-link').href = theaterInfo.href;
        recordPostEngagement(postElement, 'ViewedInTheaterOrWatchedVideo');
    } else { 
        document.querySelector('.theater-image').postElement = null;
    }

    var postsInViewport = [];
    intersectionObserver = new IntersectionObserver(
        (entries) => {
            entries.forEach(entry => {
                var post = entry.target;
                if (!post.classList.contains('post')) return;
                if (entry.isIntersecting) {
                    postsInViewport.push(post)
                    post.didAppearInViewport = true;
                } else { 
                    post.querySelectorAll('video').forEach(x => x.pause());
                    postsInViewport = postsInViewport.filter(x => x != post);
                }
                var postListElement = document.querySelector('.post-list');
                if (!postListElement) return;
                
                
                if (location.pathname != '/history') {
                    var postList = [...postListElement.children];
                    var postIndexes = postsInViewport.map(x => postList.indexOf(x)).filter(x => x != -1);
                    postIndexes.sort();
                    
                    var firstVisiblePost = postIndexes.length ? postList[postIndexes[0]] : null;
                    var postToMarkAsRead = firstVisiblePost?.previousElementSibling;
                    while (postToMarkAsRead) {
                        if (postToMarkAsRead.classList.contains('post')) {
                            if (postToMarkAsRead.wasMarkedAsRead) break;
                            if (postToMarkAsRead.didAppearInViewport) {
                                //console.log('Mark as read: ' + getPostText(postToMarkAsRead));
                                recordPostEngagement(postToMarkAsRead, 'None');
                            }
                        }
                        postToMarkAsRead = postToMarkAsRead.previousElementSibling;
                    }
                }
            });
        },
        {
            threshold: 0.1,
        }
    );
    for (const post of document.querySelectorAll('.post-list > .post')) {
        intersectionObserver.observe(post);
    }
    
    var tabbedHeader = document.querySelector('.tabbed-lists-header-inner');
    if (tabbedHeader)
        tabbedHeader.scrollLeft = previousTabbedListHeaderScrollX;

    var loginLink = document.querySelector('#login-link');
    var returnUrl = location.pathname + location.search;
    if (loginLink) 
        loginLink.href = "/login" + (location.pathname == '/login' || location.pathname == '/@bsky.app/feed/whats-hot' ? '' : '?return=' + encodeURIComponent(returnUrl))
    for (const a of document.querySelectorAll('.sidebar-item-small a[href*="/set-active-profile"]')) {
        const url = new URL(a.href)
        url.searchParams.set('return', returnUrl)
        a.href = url;
    }
    maybeLoadNextPage();
    
    if (document.querySelector('.page-error[data-islogouterror="1"]'))
        location.href = '/login?return=' + encodeURIComponent(location.pathname + location.search)

    var focalPost = document.querySelector('.post-focal');
    if (focalPost && !isTheater) {
        recordPostEngagement(focalPost, 'OpenedThread');
    }

    if (pageAutoRefreshTimeout !== null) {
        clearInterval(pageAutoRefreshTimeout);
        pageAutoRefreshTimeout = null;
    }
    if (location.pathname.endsWith("/indexing") && document.querySelector('.repository-import-row[data-pending="1"]')) { 
        pageAutoRefreshTimeout = setTimeout(() => { 
            pageAutoRefreshTimeout = null;
            fastNavigateTo(location.href, true, false);
        }, 2000);
    }
    if (location.pathname == '/debug/event-charts') { 
        pageAutoRefreshTimeout = setTimeout(() => { 
            pageAutoRefreshTimeout = null;
            fastNavigateTo(location.href, true, false);
        }, 1000);
    }
}

function tryTrimMediaSegments(href) { 
    var url = new URL(href);
    var segments = url.pathname.split('/');
    if (segments[3] == 'media') { 
        var posthandle = segments[1].substring(1);
        var postrkey = segments[2];
        var mediaId = +segments[4];

        var getImageLinkElement = () => { 
            if (posthandle.startsWith('did:'))
                return document.querySelector(getPostSelector(posthandle, postrkey) + ' a[data-theaterurl*="/media/' + mediaId + '"]');
            else
                return document.querySelector('a[data-theaterurl="/@' + posthandle + '/' + postrkey + '/media/' + mediaId + '"]');
        };
        return {
            href: new URL(url.origin + '/@' + posthandle + '/' + postrkey + url.search).href,
            posthandle,
            postrkey,
            mediaId,
            getImageLinkElement,
            getPostElement: () => getImageLinkElement()?.closest('.post')
        };
    }
    return null;
}

function trimMediaSegments(href) { 
    return tryTrimMediaSegments(href)?.href ?? href;
}

async function fetchOrReusePageAsync(href, token) { 
    href = trimMediaSegments(href);
    var index = recentPages.findIndex(x => x.href == href);
    if (index != -1) {
        var p = recentPages[index];
        recentPages.splice(index, 1);
        recentPages.push(p);
        return p;
    } else { 
        var response = await fetchCore(href, { signal: AbortSignal.timeout(20000), headers: { 'X-AppViewLiteOmitLayout': 1 } });
        if (response.status != 200) { 
            throw ('HTTP ' + response.status);
        }
        var temp = parseHtmlAsWrapper(await response.text());
        if (token != applyPageId) throw 'Superseded navigation.'
        var dom = temp.querySelector('main');
        var title = temp.querySelector('title').textContent;
        recentPages = recentPages.filter(x => x.href != response.url);
        if (new URL(response.url).pathname == '/login')
            location.href = response.url;
        var p = { href: response.url, dom: dom, dateFetched: Date.now(), title: title, scrollTop: 0 };
        recentPages.push(p)
        while (recentPages.length > 7)
            recentPages.splice(0, 1);
        return p;
    }
}

async function fastNavigateTo(href, preferRefresh = null, scrollToTop = null) { 
    if (href.startsWith('/')) href = location.origin + href;
    if (!href.startsWith(window.location.origin + '/')) { 
        window.location.href = href;
        return;
    }

    if (window.location.href == href) {
        if(scrollToTop !== false)
            window.scrollTo(0, 0);
    } else {

        if (href == historyStack[historyStack.length - 1]) { 
            applyFocusOnNextPopstate = scrollToTop == true;
            if (preferRefresh)
                recentPages = recentPages.filter(x => x.href != href);
            history.back();
            return;
        }
        historyStack.push(location.href);
        window.history.pushState(null, null, href);
    }
    await applyPage(href, preferRefresh, scrollToTop);
}

function getViewportHeight() { 
    return window.innerHeight - document.querySelector('.bottom-bar').getBoundingClientRect().height;
}

var currentlyOpenMenu = null;
var currentlyOpenMenuButton = null;
function closeCurrentMenu() { 
    if (!currentlyOpenMenu) return;
    currentlyOpenMenu.classList.remove('menu-visible')
    currentlyOpenMenu = null;
    currentlyOpenMenuButton = null;
    enableMenuFocus();
}

var scrollTopWhenMenuOrTheaterOpened = 0;

function ensureMenuFullyVisible() { 
    var menu = currentlyOpenMenu;
    menu.style.marginTop = '0px';
    menu.style.marginLeft = '0px';

    var buttonRect = currentlyOpenMenuButton.getBoundingClientRect();
    var menuRect = menu.getBoundingClientRect(); 

    scrollTopWhenMenuOrTheaterOpened = document.scrollingElement.scrollTop;
    const MIN_MARGIN = 5;

    var vw = window.innerWidth - MIN_MARGIN - 10;
    var vh = getViewportHeight() - MIN_MARGIN;

    
    var marginTop;
    var marginLeft;

    if (buttonRect.bottom + menuRect.height > vh) {
        if (menuRect.height > buttonRect.top) {
            marginTop = -buttonRect.top;
        } else {
            marginTop = -menuRect.height;
        }
    } else { 
        marginTop = buttonRect.height;
    }

    marginLeft = (buttonRect.left + buttonRect.width / 2) - menuRect.width / 2;

    if (marginLeft < MIN_MARGIN) marginLeft = MIN_MARGIN;
    if (marginLeft + menuRect.width > vw) marginLeft = vw - menuRect.width;

    marginLeft -= buttonRect.left;

    menu.style.marginTop = marginTop + 'px';
    menu.style.marginLeft = marginLeft + 'px';
}

var prevScrollTop = 0;

async function checkUpdatesForCurrentFeed() { 
    console.log('Checking updates for the current feed')
    var token = applyPageId;
    var url = new URL(location.href);
    url.searchParams.delete('limit');
    url.searchParams.append('limit', 1);
    var response = await fetchCore(url.href, { headers: { 'X-AppViewLiteUrgent': 0, 'X-AppViewLiteOmitLayout': 1 } });
    var html = await response.text();
    if (response.status != 200) return;
    if (token != applyPageId) return;
    var d = parseHtmlAsWrapper(html);

    var newPosts = [...d.querySelectorAll('.post')].map(x => x.dataset.postrkey + '|' + x.dataset.postdid)
    var existingPosts = new Set([...document.querySelectorAll('.post')].map(x => x.dataset.postrkey + '|' + x.dataset.postdid))

    if (newPosts.some(x => !existingPosts.has(x))) {
        console.log('New posts are available for the current feed.')
        document.querySelector('.scroll-up-button-badge').classList.remove('display-none');
        currentFeedHasNewPosts = true;
        currentFeedHasNewPostsTimeout = null;
    } else { 
        currentFeedHasNewPostsDelay *= 1.5;
        console.log('No new posts found, new delay: ' + currentFeedHasNewPostsDelay);
        currentFeedHasNewPostsTimeout = setTimeout(checkUpdatesForCurrentFeed, currentFeedHasNewPostsDelay);
    }
    
    
}

function clearFeedUpdateCheckTimeout() { 
    if (currentFeedHasNewPostsTimeout === null) return;
    
    console.log('Clearing feed update check timer.')
    clearTimeout(currentFeedHasNewPostsTimeout);
    currentFeedHasNewPostsTimeout = null;
}

function updateSidebarButtonScrollVisibility() { 
    var scrollTop = document.documentElement.scrollTop;
    var scrollDelta = scrollTop - prevScrollTop;
    if (Math.abs(scrollDelta) >= 3) {
        document.querySelector('.sidebar-button').classList.toggle('sidebar-button-fixed', scrollDelta < 0);
        prevScrollTop = scrollTop;
    }
    var showScrollUp = scrollTop >= 700 && location.pathname.split('/')[1] != 'settings';
    document.querySelector('.scroll-up-button').classList.toggle('display-none', !showScrollUp);

    var url = new URL(location.href);
    var path = url.pathname;

    var needsScrollUpTimer =
        !currentFeedHasNewPosts &&
        showScrollUp &&
        ((path == '/following' && url.searchParams.get('sort') == 'recent') || path.includes('/feed/') || path == '/firehose');
    
    if ((currentFeedHasNewPostsTimeout !== null) != needsScrollUpTimer) { 
        if (needsScrollUpTimer) {
            currentFeedHasNewPostsDelay = 20000;
            console.log('Setting up initial timer for feed update check: ' + currentFeedHasNewPostsDelay)

            currentFeedHasNewPostsTimeout = setTimeout(async () => {
                checkUpdatesForCurrentFeed();
            }, currentFeedHasNewPostsDelay);

        } else { 
            clearFeedUpdateCheckTimeout()
        }

    }
    
}

function switchTheaterImage(delta, allowWrap = true) { 
    var theater = tryTrimMediaSegments(location.href);
    if (!theater) return false;
    var imageCount = theater.getPostElement().querySelector('.post-image-list').children.length;
    var mediaId = theater.mediaId + delta;
    if (mediaId == 0) {
        if (!allowWrap) return false;
        mediaId = imageCount;
    }
    else if (mediaId == imageCount + 1) {
        if (!allowWrap) return false;
        mediaId = 1;
    }
    var prevReturn = theaterReturnUrl;
    fastNavigateTo('/@' + theater.posthandle + '/' + theater.postrkey + '/media/' + mediaId, false, false);
    theaterReturnUrl = prevReturn;
    return true;
}

var SPINNER_HTML = '<div class="spinner"><svg height="100%" viewBox="0 0 32 32" width="100%"><circle cx="16" cy="16" fill="none" r="14" stroke-width="4" style="stroke: currentColor; opacity: 0.2;"></circle><circle cx="16" cy="16" fill="none" r="14" stroke-width="4" style="stroke: currentColor; stroke-dasharray: 80px; stroke-dashoffset: 60px;"></circle></svg></div>';

function focusPostForKeyboardNavigation(post, isFirst) { 
    if (!post) return;
    var bg = post.querySelector('.post-background-link');
    bg.focus();
    if(isFirst) window.scrollTo(0, 0)
    else post.scrollIntoView();
}


  
async function loadNextPage(allowRetry) { 
    var paginationButton = document.querySelector('.pagination-button');
    if (!paginationButton) return;
    if (!allowRetry && paginationButton.classList.contains('pagination-button-retry')) return;
    if (paginationButton.querySelector('.spinner')) return;
    var oldList = document.querySelector('.main-paginated-list');
    paginationButton.querySelector('.pagination-button-error-details')?.remove();


    paginationButton.classList.add('spinner-visible')
    paginationButton.insertAdjacentHTML('beforeend', SPINNER_HTML)

    try {
        var nextPage = await fetchCore(paginationButton.querySelector('a').href, { signal: AbortSignal.timeout(20000), headers: { 'X-AppViewLiteOmitLayout': 1 } });
        if (nextPage.status != 200) throw ('HTTP ' + nextPage.status);
        var temp = parseHtmlAsWrapper(await nextPage.text());
        var pageError = temp.querySelector('.page-error')?.textContent;
        if (pageError) throw pageError
    } catch (e) { 
        paginationButton.querySelector('a').textContent = 'Retry'
        paginationButton.classList.add('pagination-button-retry');
        paginationButton.classList.remove('spinner-visible');

        var errorDetails = document.createElement('div');
        errorDetails.classList.add('pagination-button-error-details');
        errorDetails.textContent = getDisplayTextForException(e) || 'Unknown error.';
        paginationButton.appendChild(errorDetails);
        paginationButton.querySelector('.spinner').remove();
        return;
    }

    var newList = temp.querySelector('.main-paginated-list');
    var anyChildren = false;
    if (newList) {
        for (const child of [...newList.childNodes]) {
            child.remove();
            if (child instanceof Element) anyChildren = true;
            oldList.appendChild(child);
            intersectionObserver.observe(child);
        }
    }
    var newPagination = temp.querySelector('.pagination-button');
    if (!newPagination || !anyChildren) paginationButton.remove();
    else paginationButton.replaceWith(newPagination);

    if (anyChildren) {
        updateLiveSubscriptions();
        maybeLoadNextPage();
    }
}

var forceShowAdvancedMenuItems = 0;


function maybeLoadNextPage() { 
    var scrollingElement = document.scrollingElement;
    var scrollTop = scrollingElement.scrollTop
    var scrollTopMax = scrollingElement.scrollHeight - scrollingElement.clientHeight;
    var remainingToBottom = scrollTopMax - scrollTop;
    if (remainingToBottom >= 500) return;
    loadNextPage(false);
}

function enableMenuFocus() { 
    for (const element of document.querySelectorAll('.menu-item-hide-focus')) {
        element.classList.remove('menu-item-hide-focus');
    }
}

function recurseOnceWhereVisible(node, f) { 
    while (node) { 
        node = f(node);
        if (node && node.offsetParent)
            return node;
    }
    return null;
}
function recurseUntilVisible(node, f) { 
    while (node && !node.offsetParent) { 
        node = f(node);
    }
    return node;
}

function onMiddleClickOrLongPress(e, middleClick) { 
    var postElement = e.target.closest('.post');
    if (postElement) {
        if (e.target.classList?.contains('post-body')) {
            var href = getPostPreferredUrl(e.target.parentElement);
            if (href) {
                recordPostEngagement(postElement, 'OpenedThread');
                if (middleClick) {
                    window.open(href);
                    e.preventDefault();
                }
            }
        } else {
            var a = e.target.closest('a');
            if (a) {
                if (a.classList.contains('blue-link') && a.closest('.post-body')) {
                    recordPostEngagement(postElement, 'OpenedExternalLink');
                }
                if (a.classList.contains('post-external-preview')) {
                    recordPostEngagement(postElement, 'OpenedExternalLink');
                }
                if (a.classList.contains('post-background-link')) { 
                    recordPostEngagement(postElement, 'OpenedThread');
                }
                if (a.classList.contains('post-date')) { 
                    recordPostEngagement(postElement, 'OpenedThread');
                }
                if (a.classList.contains('post-menu-advanced-open')) { 
                    recordPostEngagement(postElement, 'OpenedThread');
                }
            }
        }
    }

}

function onInitialLoad() {
    if (isNoLayout) return;

    var ssr = document.documentElement.dataset.ssrPageUrl;
    if (ssr) { 
        var originAspNet = new URL(ssr).origin;
        var originJs = location.origin;
        if (originAspNet != originJs) { 
            document.querySelector('.sidebar').insertAdjacentHTML('afterBegin', "<div style=\"font-size: 8pt; color: rgb(126, 27, 27);\">Your reverse proxy is not forwarding X-Forwarded-For, X-Forwarded-Proto, X-Forwarded-Host.<br><br>Some generated links might be broken (" + originJs + " vs " + originAspNet + ")<br><br>If your reverse proxy is not on localhost, you might also need to configure ForwardedHeadersOptions.KnownProxies</div>")
        }
    }

    window.addEventListener('beforeunload', e => {
        console.log('beforeunload event triggered.')
    });
    window.addEventListener('popstate', e => {
        var popped = historyStack.pop();
        if (popped != location.href) { 
            console.log("History stack (" + popped +") / pushState (" + location.href + ") mismatch");
        }
        applyPage(location.href, false, applyFocusOnNextPopstate ? true : false);
        applyFocusOnNextPopstate = false;
    });
    
    appliedPageObj = {
        href: trimMediaSegments(location.href),
        dateFetched: Date.now(),
        dom: document.querySelector('main'),
        title: document.title,
        scrollTop: document.scrollingElement.scrollTop
    };
    recentPages.push(appliedPageObj);
    
    window.visualViewport.addEventListener('resize', () => {
        visualViewportWasResizedSinceLastTheaterOrMenuOpen = true;
    });

    window.addEventListener('scroll', e => {
        if (pageLoadedTimeBeforeInitialScroll !== null) { 
            var scrollTop = document.scrollingElement.scrollTop;
            if ((Date.now() - pageLoadedTimeBeforeInitialScroll) < 5000 && scrollTop > 200) {
                // Firefox restored the tab. However, the scroll position is no longer meaningful (infinite scroll).
                console.log('Detected probable Firefox tab restore, resetting scroll position')
                document.scrollingElement.scrollTop = 0;
            }
            pageLoadedTimeBeforeInitialScroll = null;
        }
        updateSidebarButtonScrollVisibility();
        
        if (window.visualViewport.scale == 1 && !visualViewportWasResizedSinceLastTheaterOrMenuOpen) {
            if (Math.abs(scrollTopWhenMenuOrTheaterOpened - document.scrollingElement.scrollTop) > 10) {
                closeCurrentMenu();
                closeTheater();
            }
        }
        maybeLoadNextPage();
    }, { passive: true });

    document.addEventListener('keydown', e => {
        if (e.ctrlKey || e.shiftKey || e.altKey) return;
        
        if (e.target?.id == 'search-box' && e.key == 'ArrowDown') { 
            document.querySelector('.search-form-suggestion')?.focus();
            e.preventDefault();
            return;
        }

        if (currentlyOpenMenu || document.querySelector('.search-form-suggestions')?.firstElementChild) { 
            var currentMenuItem = document.activeElement;
            if (currentMenuItem && (currentMenuItem.classList.contains('menu-item') || currentMenuItem.classList.contains('search-form-suggestion'))) {
                enableMenuFocus();

                if (e.key == 'ArrowUp') {
                    let previous = recurseOnceWhereVisible(currentMenuItem, x => x.previousElementSibling);
                    if (previous) {
                        previous.focus();
                    } else {
                        if (currentMenuItem.classList.contains('search-form-suggestion')) document.querySelector('#search-box').focus();
                        else recurseUntilVisible(currentMenuItem.parentElement.lastElementChild, x => x.previousElementSibling).focus();
                    }
                    e.preventDefault();
                    return;
                } else if (e.key == 'ArrowDown') {
                    let next = recurseOnceWhereVisible(currentMenuItem, x => x.nextElementSibling);
                    if (next) {
                        next.focus();
                    } else {
                        recurseUntilVisible(currentMenuItem.parentElement.firstElementChild, x => x.nextElementSibling).focus();
                    }
                    e.preventDefault();
                    return;
                }
            }
        }

        if (e.key == 'Escape') { 
            if (tryTrimMediaSegments(location.href)) {
                closeTheater();
            } else {
                closeCurrentMenu();
                closeAutocompleteMenu();
            }
            e.preventDefault();
        }
        if (e.key == ' ' || e.key == 'Enter') { 
            if(tryTrimMediaSegments(location.href)) {
                closeTheater();
                e.preventDefault();
                return;
            }
        }
        var target = e.target;
        var targetIsInput = target.tagName == 'INPUT' || target.tagName == 'TEXTAREA' || 
            (target.tagName == 'BUTTON' && (e.key == 'Enter' || e.key == ' '));


        if (!targetIsInput) {
            var num = e.key;
            if (num.length == 1 && num.charCodeAt(0) >= 48 && num.charCodeAt(0) <= 57) {
                // 0..9
                var index = num == '0' ? 10 : (+num - 1);
                var option = document.querySelectorAll('.sidebar nav > div a')[index];
                if (option) {
                    option.click();
                    e.preventDefault();
                }
            }
        }
            

        var onlyIfCurrentPostHasUrl = null;
        if (!targetIsInput) { 
            if (e.key == 'Enter' && target.tagName == 'A') { 
                
                onlyIfCurrentPostHasUrl = target.href;
            }
        }

        if (!targetIsInput) { 
            if (e.key == 'j' || e.key == 'k' || e.key == 'Enter') { 
                if (e.key == 'j' && switchTheaterImage(1, false)) { e.preventDefault(); return; }
                if (e.key == 'k' && switchTheaterImage(-1, false)) { e.preventDefault(); return; }
                closeTheater();
                var posts = document.querySelectorAll('.post-list > .post');
                for (let i = 0; i < posts.length; i++) {
                    const post = posts[i];
                    const rect = post.getBoundingClientRect();

                    if (e.key == 'k' && rect.top >= -10) {
                        focusPostForKeyboardNavigation(posts[i - 1], i == 1);
                        e.preventDefault();
                        break;
                    } else if (e.key == 'j' && rect.bottom >= 10) {
                        focusPostForKeyboardNavigation(posts[i + 1]);
                        e.preventDefault();
                        break;
                    } else if (e.key == 'Enter' && ((rect.top + rect.bottom) / 2) >= 0) { 

                        var postUrl = '/@' + post.dataset.postdid + '/' + post.dataset.postrkey;
                        if (!onlyIfCurrentPostHasUrl || onlyIfCurrentPostHasUrl == new URL(location.origin + postUrl).href) {
                            var link = post.querySelector('.post-image-link');
                            if (link) {
                                link.click();
                            } else {
                                fastNavigateTo(postUrl);
                            }
                            e.preventDefault();
                            break;
                        }
                    }
                }
            }
            if (e.key == 'Backspace') { 
                history.back();
            }
        }



        if (e.key == 'ArrowRight') { 
            if (switchTheaterImage(1))
                e.preventDefault();
        }
        if (e.key == 'ArrowLeft') { 
            if (switchTheaterImage(-1))
                e.preventDefault();
        }



    });

    document.addEventListener('mousemove', e => {
        var target = e.target;
        if (target && target instanceof Element) { 
            var menuItem = target.closest('.menu-item,.search-form-suggestion');
            if (menuItem) { 
                menuItem.focus();
                enableMenuFocus();
            }
            
        }
    });

    document.addEventListener('pointerdown', e => {
        var menuItem = e.target?.closest('.menu-item');
        if (menuItem) { 
            menuItem.focus(); // for visual feedback
        }
    });

    document.addEventListener('mousedown', e => { 
        userSelectedTextSinceLastMouseDown = false;
        if (e.button == 1) { 
            onMiddleClickOrLongPress(e, true);
        }
    })


    document.addEventListener('contextmenu', e => {
        if (e.target?.classList?.contains('post-action-bar-button') && !e.shiftKey) { 
            closeCurrentMenu();
            e.preventDefault();
            forceShowAdvancedMenuItems++;
            try {
                e.target.click();
            } finally {
                forceShowAdvancedMenuItems--;
            }
            return;
        }
        var target = e.target;
        if (target.classList?.contains('theater-image')) {
            recordPostEngagement(target.postElement, 'Downloaded');
        } else if (target.classList?.contains('post-image')) { 
            recordPostEngagement(target.closest('.post'), 'Downloaded');
        }
        onMiddleClickOrLongPress(e, false);
    });

    document.addEventListener('click', e => {
        if (e.ctrlKey) return;

        var target = e.target;
        var clickFromKeyboard = e.detail === 0;

        var showAdvanced = e.shiftKey || forceShowAdvancedMenuItems;

        if (currentlyOpenMenu) { 
            if (currentlyOpenMenuButton.contains(target)) { 
                closeCurrentMenu();
                if (!showAdvanced || document.body.classList.contains('show-advanced-menu-items'))
                    return;
            }
            if (currentlyOpenMenu?.contains(target)) { 
                closeCurrentMenu();
            }
        }

        if (fastNavigateIfLink(e))
            return;

        if (target.classList?.contains('post-background-link') || target.classList?.contains('post-date') || target.classList?.contains('post-menu-advanced-open')) { 
            var post = target.closest('.post');
            recordPostEngagement(post, 'OpenedThread');
        }

        if (target.parentElement?.classList?.contains('post-link-to-external-thread')) { 
            var post = document.querySelector('.post-focal');
            if (post)
                recordPostEngagement(post, 'OpenedThread');
        }


        var actionButton = target.closest('.post-action-bar-button,[actionkind]');
        closeCurrentMenu();
        if (actionButton) { 
            var actionKind = actionButton.getAttribute('actionkind');
            if (actionKind) {
                console.log(actionKind);
                var postElement = actionButton.closest('[data-postrkey]');
                var profileElement = actionButton.closest('[data-profiledid]');
                var feedElement = actionButton.closest('[data-feeddid]');
                var listElement = actionButton.closest('[data-moderationdid]');
                if (postElement == profileElement) profileElement = null;

                if (postElement) {
                    var postAction = postActions[actionKind];
                    if (postAction) {
                        postAction.call(postActions,
                            getAncestorData(actionButton, 'postdid'),
                            getAncestorData(actionButton, 'postrkey'),
                            postElement,
                            actionButton
                        );
                        return;
                    }
                }
                if(profileElement) {
                    var userAction = userActions[actionKind];
                    if (userAction) {
                        userAction.call(userActions,
                            getAncestorData(actionButton, 'profiledid'),
                            actionButton.closest('[data-profiledid]'),
                            actionButton
                        );
                        return;
                    }
                }
                if(feedElement) {
                    var feedAction = feedActions[actionKind];
                    if (feedAction) {
                        feedAction.call(feedActions,
                            getAncestorData(actionButton, 'feeddid'),
                            getAncestorData(actionButton, 'feedrkey'),
                            actionButton.closest('[data-feeddid]'),
                            actionButton
                        );
                        return;
                    }
                }
                if(listElement) {
                    var listAction = listActions[actionKind];
                    if (listAction) {
                        listAction.call(listElement,
                            getAncestorData(actionButton, 'moderationdid'),
                            getAncestorData(actionButton, 'moderationlistrkey'),
                            getAncestorData(actionButton, 'moderationlabelname'),
                            actionButton.closest('[data-moderationdid]'),
                            actionButton
                        );
                        return;
                    }
                }
            } else {

                if (actionButton == currentlyOpenMenuButton) closeCurrentMenu();
                else {
                    var prevMenu = actionButton.previousElementSibling;
                    if (prevMenu && prevMenu.classList.contains('menu')) {
                        prevMenu.classList.add('menu-visible');
                        currentlyOpenMenuButton = actionButton;
                        currentlyOpenMenu = prevMenu;
                        visualViewportWasResizedSinceLastTheaterOrMenuOpen = false;
                        document.body.classList.toggle('show-advanced-menu-items', showAdvanced);
                        ensureMenuFullyVisible();
                        var first = currentlyOpenMenu.querySelector('a, button');
                        if (first) {
                            if (!clickFromKeyboard)
                                first.classList.add('menu-item-hide-focus');
                            setTimeout(() => first.focus(), 0);
                        }
                    }
                }
            }
        }

        var autocomplete = target.closest('.search-form-suggestions');
        if (!autocomplete)
            closeAutocompleteMenu();
    });

    
    applyPageElements();
    applyPageFocus();
    
    onPageScrollPositionFinalized();

    prevScrollTop = 0;
    updateSidebarButtonScrollVisibility();
}

function onPageScrollPositionFinalized() { 
    visualViewportWasResizedSinceLastTheaterOrMenuOpen = false;
    scrollTopWhenMenuOrTheaterOpened = document.scrollingElement.scrollTop;
}

function getAncestorData(target, name) { 

    while (target) { 
        var d = target.dataset[name];
        if (d) return d;
        target = target.parentElement;
    }
    throw 'Data attribute not found: ' + name
}

function scrollToTopAndRefresh() { 
    window.scrollTo(0, 0);
    fastNavigateTo(location.href);
}


class ActionStateToggler { 
    constructor(actorCount, rkey, addRelationship, deleteRelationship, notifyChange) { 
        if (!rkey || rkey == '-') rkey = null;
        this.actorCount = actorCount;
        this.rkey = rkey;
        this.addRelationship = addRelationship;
        this.deleteRelationship = deleteRelationship;
        this.busy = false;
        this.haveRelationship = !!rkey;
        this.notifyChange = notifyChange;
    }

    applyLiveUpdate(actorCount, rkey) { 
        this.actorCount = actorCount;
        if (rkey) { // null means leave unchanged
            if (!rkey || rkey == '-') rkey = null;
            this.haveRelationship = !!rkey;
        }
        this.rkey = rkey;
        this.notifyChange(this.actorCount, this.haveRelationship);
    }

    raiseChangeNotification(){ 
        this.notifyChange(this.actorCount, this.haveRelationship);
    }
    async toggleIfNotBusyAsync() {
        if (this.busy) return;
        this.busy = true;
        var prevState = [this.haveRelationship, this.rkey, this.actorCount];
        try {
            this.haveRelationship = !this.haveRelationship;

            if (this.haveRelationship) {
                this.actorCount++;
                this.raiseChangeNotification();
                this.rkey = await this.addRelationship();
            } else { 
                if (this.actorCount > 0) { 
                    this.actorCount--;
                }
                this.raiseChangeNotification();
                await this.deleteRelationship(this.rkey);
            }
            this.raiseChangeNotification();
        } catch (e) {
            [this.haveRelationship, this.rkey, this.actorCount] = prevState;
            this.raiseChangeNotification();
            console.error(getDisplayTextForException(e));
        } finally { 
            this.busy = false;
        }
    }
}

async function fetchCore(input, init) { 
    init ??= {};
    init.headers ??= {};
    init.headers['X-AppViewLitePreferredDid'] = document.documentElement.dataset.currentlyactivedid;
    try {
        return await fetch(input, init);
    } catch (e) {
        if (init?.signal?.aborted)
            throw 'AppViewLite did not respond in a timely fashion.';
        throw e;
    }
}


async function httpPost(method, args) { 
    var response = await fetchCore('/api/' + method, {
        body: JSON.stringify(args),
        headers: {
            'Content-Type': 'application/json',
            'X-AppViewLiteSignalrId': liveUpdatesConnection?.connectionId
        },
        method: 'POST',
        signal: AbortSignal.timeout(5000)
    })
    if (response.status != 200) throw 'HTTP ' + response.status;
    var text = await response.text();
    if (!text) return null;
    return JSON.parse(text);
}

async function httpGet(method, args) { 
    var response = await fetchCore('/api/' + method + '?' + new URLSearchParams(args).toString(), {
        method: 'GET',
        headers: {
            'X-AppViewLiteSignalrId': liveUpdatesConnection?.connectionId
        },
        signal: AbortSignal.timeout(20000)
    })
    if (response.status != 200) throw 'HTTP ' + response.status;
    var text = await response.text();
    if (!text) return null;
    return JSON.parse(text);
}

function animateReplaceContent(b, formattedNumber) { 
    if (!b) return;
    if (b.textContent == formattedNumber) return;
    b.style.opacity = 0;
    setTimeout(() => {
        b.textContent = formattedNumber;
        b.style.opacity = 1;
    }, 250);
}

function setPostStats(postElement, actorCount, kind, singular, plural) { 
    var stats = postElement.querySelector('.post-stats-' + kind + '-formatted');
    if (!stats) return;
    stats.classList.toggle('display-none', !actorCount);
    var b = stats.firstElementChild;
    var text = stats.lastChild;
    animateReplaceContent(b, formatEngagementCount(actorCount));
    text.replaceWith(document.createTextNode(' ' + (actorCount == 1 ? singular : plural)));
    var allStats = postElement.querySelector('.post-focal-stats');
    if (allStats)
        allStats.classList.toggle('display-none', [...allStats.children].every(x => x.classList.contains('display-none')));
}
function setActionStats(postElement, actorCount, kind) { 
    animateReplaceContent(postElement.querySelector('.post-action-bar-button-' + kind + ' span'), actorCount ? formatEngagementCount(actorCount) : '');
}


function getOrCreateLikeToggler(did, rkey, postElement) { 
    var prevKey = '';
    return postElement.likeToggler ??= new ActionStateToggler(
        +postElement.dataset.likecount,
        postElement.dataset.likerkey,
        async () => { 
            recordPostEngagement(postElement, 'LikedOrBookmarked');
            return (await httpPost('CreatePostLike', { did, rkey, viaDid: postElement.dataset.repostedby, viaRkey: postElement.dataset.repostedbyrkey })).rkey;
        },
        async (rkey) => (await httpPost('DeletePostLike', { rkey })),
        (count, have) => { 
            var key = formatEngagementCount(count) + have.toString();
            if (key == prevKey) return;
            prevKey = key;
            setPostStats(postElement, count, 'likes', 'like', 'likes');
            if (isNativeDid(did))
                setActionStats(postElement, count, 'like');
            var likeButton = postElement.querySelector('.post-action-bar-button-like');
            if (likeButton && postElement.dataset.usebookmarkbutton == '0')
                likeButton.classList.toggle('post-action-bar-button-checked', have);
        });
}



function getOrCreateBookmarkToggler(did, postRkey, postElement) { 
    return postElement.bookmarkToggler ??= new ActionStateToggler(
        0,
        postElement.dataset.bookmarkrkey,
        async () => {
            recordPostEngagement(postElement, 'LikedOrBookmarked');
            return (await httpPost('CreatePostBookmark', { did, rkey: postRkey })).rkey;
        },
        async (bookmarkRkey) => (await httpPost('DeletePostBookmark', { bookmarkRkey: bookmarkRkey, postDid: did, postRkey: postRkey })),
        (count, have) => { 
            
            var likeButton = postElement.querySelector('.post-action-bar-button-like');
            if (likeButton && postElement.dataset.usebookmarkbutton == '1')
                likeButton.classList.toggle('post-action-bar-button-checked', have);

            var toggleBookmarkMenuItem = postElement.querySelector('.menu-item[actionkind="toggleBookmark"]');
            if (toggleBookmarkMenuItem)
                toggleBookmarkMenuItem.textContent = have ? 'Remove bookmark' : 'Add bookmark';
        });
}

function getOrCreateRepostToggler(did, rkey, postElement) { 
    var prevKey = '';
    return postElement.repostToggler ??= new ActionStateToggler(
        +postElement.dataset.repostcount,
        postElement.dataset.repostrkey,
        async () => {
            recordPostEngagement(postElement, 'LikedOrBookmarked');
            return (await httpPost('CreateRepost', { did, rkey, viaDid: postElement.dataset.repostedby, viaRkey: postElement.dataset.repostedbyrkey })).rkey
        },
        async (rkey) => (await httpPost('DeleteRepost', { rkey })),
        (count, have) => { 
            var quoteCount = +postElement.dataset.quotecount;
            var key = formatEngagementCount(count) + '/' + formatEngagementCount(quoteCount) + '/' + formatEngagementCount(quoteCount + count) + have.toString();
            if (key == prevKey) return;
            prevKey = key;
            setPostStats(postElement, count, 'reposts', 'repost', 'reposts');
            setPostStats(postElement, quoteCount, 'quotes', 'quote', 'quotes');
            setActionStats(postElement, count + quoteCount, 'repost');
            var repostButton = postElement.querySelector('.post-action-bar-button-repost');
            if (repostButton)
                repostButton.classList.toggle('post-action-bar-button-checked', have);
            var repostMenuItem = postElement.querySelector('.post-toggle-repost-menu-item');
            if (repostMenuItem)
                repostMenuItem.textContent = have ? 'Undo repost' : 'Repost'
        });
}

function getPostText(postElement) { 
    return getTextIncludingEmojis([...postElement.children].filter(x => x.classList.contains('post-body'))[0]).trim();
}

function getPostPreferredUrl(postElement) { 
    return getPostPreferredUrlElement(postElement).href;
}
function getPostPreferredUrlElement(postElement) { 
    return [...postElement.children].filter(x => x.classList.contains('post-background-link'))[0];
}

function invalidateFollowingPages() { 
    recentPages = recentPages.filter(x => !(x.href.includes('/following') || x.href.includes('/followers') || x.href.includes('/known-followers')));
}
function invalidateLikesPages() { 
    recentPages = recentPages.filter(x => !(x.href.includes('likes=1') || x.href.includes('bookmarks=1')));
}
function invalidateFeedPages() { 
    recentPages = recentPages.filter(x => !(x.href.includes('/search') || x.href.includes('/feed/')));
}
function invalidateComposePage() { 
    recentPages = recentPages.filter(x => !(x.href.includes('/compose')));
}

async function togglePrivateFollow(did, toggleButton, postElement) { 
    var did = toggleButton.dataset.did;
    var flag = toggleButton.dataset.flag;
    var oldvalue = !!(+toggleButton.dataset.oldvalue);
    var text = toggleButton.textContent;
    text = text.substring(text.indexOf(' '));
    var newvalue = !oldvalue;
    await httpPost('TogglePrivateFollow', { did, flag, newvalue });
    toggleButton.dataset.oldvalue = newvalue ? 1 : 0;
    toggleButton.textContent = (oldvalue ? 'Mute ' : 'Unmute ') + text;
    postElement.classList.toggle('post-muted', newvalue);
    invalidateFollowingPages();
}

async function postToScreenshotBlob(postElement) {
    document.documentElement.classList.add('force-blue-link-accent-color');
    try {
    /**@type {HTMLCanvasElement}*/ var canvas = await html2canvas(postElement, {
        imageTimeout: 2000,
        backgroundColor: window.getComputedStyle(document.documentElement).backgroundColor
    });
    } finally { 
        document.documentElement.classList.remove('force-blue-link-accent-color');
    }
    
    return await new Promise(resolve => canvas.toBlob(resolve, "image/png"));
}


async function composeWithScreenshot(postElement) {
    var screenshotBlob = await postToScreenshotBlob(postElement);
    await fastNavigateTo('/compose');
    clearFileUploads();
    var postText = getPostText(postElement);
    var authorHandleElement = postElement.querySelector('.handle-generic');
    var authorHandle = authorHandleElement.textContent;
    if (!authorHandleElement.classList.contains('handle-without-at-prefix')) { 
        authorHandle = '@' + authorHandle;
    }
    var authorHandlePrefix = authorHandle + ': ';
    if (postText) {
        await composeAddFile(new File([screenshotBlob], 'image.png'), authorHandlePrefix + postText);
    } else {
        await composeAddFile(new File([screenshotBlob], 'image.png'));
    }
}

var postActions = {
    toggleLike: async function (did, rkey, postElement) { 
        if (postElement.dataset.usebookmarkbutton == '1')
            getOrCreateBookmarkToggler(did, rkey, postElement).toggleIfNotBusyAsync();
        else
            getOrCreateLikeToggler(did, rkey, postElement).toggleIfNotBusyAsync();
        invalidateLikesPages();
    },
    toggleRepost: async function (did, rkey, postElement) { 
        if (isNativeDid(did)) {
            getOrCreateRepostToggler(did, rkey, postElement).toggleIfNotBusyAsync();
        } else { 
            composeWithScreenshot(postElement);
        }
        
    },
    composeReply: async function (did, rkey, postElement) {
        if (isNativeDid(did)) fastNavigateTo(`/compose?replyDid=${did}&replyRkey=${rkey}`);
        else composeWithScreenshot(postElement);
    },
    composeQuote: async function (did, rkey, postElement) { 
        if (isNativeDid(did)) fastNavigateTo(`/compose?quoteDid=${did}&quoteRkey=${rkey}`);
        else composeWithScreenshot(postElement);
    },
    viewOnBluesky: async function (did, rkey) { 
        window.open(`https://bsky.app/profile/${did}/post/${rkey}`);
    },
    deletePost: async function (did, rkey, node) { 
        await httpPost('DeletePost', { rkey });
        var nextSeparator = node.nextElementSibling;
        if (nextSeparator?.classList.contains('post-group-separator')) nextSeparator.remove();
        node.remove();
        if (!document.querySelector('.post'))
            fastNavigateTo('/');
    },
    hideThreadReply: async function (did, rkey, node, button) { 
        node.classList.add('post-blocked');
        if (!node.querySelector('.post-blur-reason')) {
            const blockReason = document.createElement('div');
            blockReason.className = 'post-blur-reason';
            blockReason.textContent = 'This reply was hidden by the thread author.';
            const blockReasonContainer = document.createElement('div');
            blockReasonContainer.className = 'post-blur-reason-container';
            blockReasonContainer.appendChild(blockReason);
            const pos = node.querySelector('.post-author-image-link-container');
            if (pos)
                pos.parentElement.insertBefore(blockReasonContainer, pos);
        }
        await httpPost('HideThreadReply', { did, rkey });
        button.remove();
    },
    copyPostUrl: async function (did, rkey) { 
        navigator.clipboard.writeText(location.origin + '/@' + did + '/' + rkey)
    },
    copyOriginalPostUrl: async function (did, rkey, node) { 
        navigator.clipboard.writeText(getPostPreferredUrl(node))
    },
    
    copyBlueskyPostUrl: async function (did, rkey) { 
        navigator.clipboard.writeText('https://bsky.app/profile/' + did + '/post/' + rkey);
    },
    translatePost: async function (did, rkey, postElement) { 
        window.open('https://translate.google.com/?sl=auto&tl=en&text=' + encodeURIComponent(getPostText(postElement)) + '&op=translate');
    },
    toggleBookmark: async function (did, rkey, postElement) { 
        await getOrCreateBookmarkToggler(did, rkey, postElement).toggleIfNotBusyAsync();
        invalidateLikesPages();
    },
    togglePrivateFollow: async function (did, rkey, postElement, toggleButton) { 
        await togglePrivateFollow(did, toggleButton, postElement);
    },
    muteThread: async function (did, rkey, postElement, muteButton) {
        await httpPost('MuteThread', { did: muteButton.dataset.threaddid, rkey: muteButton.dataset.threadrkey });
        muteButton.remove();
        postElement.classList.toggle('post-muted', true);
    },
    muteDomain: async function (did, rkey, postElement, muteButton) { 
        var domain = muteButton.dataset.mutedomain;
        var muted = muteButton.dataset.ismuted == '1';
        muted = !muted;
        postElement.classList.toggle('post-muted', muted);
        await httpPost('ToggleDomainMute', { domain: domain, mute: muted });
        muteButton.dataset.ismuted = muted ? '1' : '0';
        muteButton.textContent = (muted ? 'Unmute' : 'Mute') + ' links to ' + domain;
    },
    toggleFastVideoPlayback: function (did, rkey, postElement, toggleButton) { 
        var videos = postElement.querySelectorAll('video');
        var video = [...videos].filter(x => !x.paused)[0] ?? videos[0];
        if (video.playbackRate == 1) {
            video.playbackRate = 1.75;
            toggleButton.textContent = 'Slow down playback';
        } else { 
            video.playbackRate = 1;
            toggleButton.textContent = 'Speed up playback';
        }
        if (video.paused)
            video.play();
    }
}

function isNativeDid(did) { 
    return did.startsWith('did:plc:') || did.startsWith('did:web:');
}

var userActions = {
    toggleFollow: async function (profiledid, profileElement, button) { 
        
        var followrkey = profileElement.dataset.followrkey;
        var followsyou = profileElement.dataset.followsyou;
        var isPrivateFollow = button.dataset.followkind == 'private';
        var mandatoryPrivateFollow = !isNativeDid(profiledid)

        profileElement.followToggler ??= new ActionStateToggler(
            0,
            followrkey,
            async () => (await httpPost('CreateFollow', { did: profiledid, private: profileElement.followPrivately })).rkey,
            async (rkey) => (await httpPost('DeleteFollow', { did: profiledid, rkey })),
            (count, have) => { 
                var followButton = profileElement.querySelector('.follow-button');
                if (followButton) {
                    followButton.textContent = have ? 'Following' : +followsyou ? 'Follow back' : 'Follow';
                    followButton.classList.toggle('follow-button-unfollow', have);
                    followButton.classList.toggle('follow-button-private', profileElement.followPrivately)
                }
                var followPrivately = profileElement.querySelector('.menu-item[actionkind="toggleFollow"][data-followkind="private"]');
                if (followPrivately) { 
                    followPrivately.textContent = have ? 'Unfollow (private)' : 'Follow privately'
                    followPrivately.classList.toggle('display-none', have && !profileElement.followPrivately);
                }
        });
        profileElement.followPrivately = mandatoryPrivateFollow || (isPrivateFollow && !profileElement.followToggler.haveRelationship);
        profileElement.followToggler.toggleIfNotBusyAsync();
        invalidateFollowingPages();
    },

    toggleBlock: async function (profiledid, profileElement, button) { 
        var blockrkey = profileElement.dataset.blockrkey;
        if (blockrkey != '-') {
            await httpPost('DeleteBlock', { did: profiledid, rkey: blockrkey });
        } else { 
            await httpPost('CreateBlock', { did: profiledid });
        }
        location.reload();
    },
    
    copyProfileUrl: async function (did) { 
        navigator.clipboard.writeText(location.origin + '/@' + did)
    },
    copyProfileBlueskyUrl: async function (did) { 
        navigator.clipboard.writeText('https://bsky.app/profile/' + did)
    },
    togglePrivateFollow: async function (did, profileElement, toggleButton) { 
        await togglePrivateFollow(did, toggleButton);
    }
}


var feedActions = {
    toggleFeedPin: async function (did, rkey, feedElement) {
        if (+feedElement.dataset.ispinned) {
            await httpPost('UnpinFeed', { did: did, rkey: rkey });
            feedElement.dataset.ispinned = 0
        } else { 
            await httpPost('PinFeed', { did: did, rkey: rkey });
            feedElement.dataset.ispinned = 1
        }
        feedElement.querySelectorAll('[actionkind="toggleFeedPin"]').forEach(x => {
            x.textContent = +feedElement.dataset.ispinned ? 'Unpin feed' : 'Pin feed';
        });
        invalidateFeedPages();
    },
}

var listActions = {
    setLabelerMode: async function (did, listrkey, labelName, listElement, buttonElement) {
        var mode = buttonElement.dataset.mode;
        await httpPost('SetLabelerMode', { did: did, listRkey: listrkey != '-' ? listrkey : null, labelName: labelName != '-' ? labelName : null, mode: mode, nickname: null });
    
        buttonElement.parentElement.querySelectorAll('.labeler-mode').forEach(x => x.classList.remove('labeler-mode-active'));
        buttonElement.classList.add('labeler-mode-active');
    },
    setLabelerPrivateName: async function (did, listrkey, labelName, listElement, buttonElement) {
        var newName = prompt('Choose a new name for this list:', buttonElement.dataset.nickname ?? '');
        if (newName === null) return;
        newName = newName.trim();
        if (!newName || newName == buttonElement.dataset.originalname) { 
            newName = '';
        }
        await httpPost('SetLabelerMode', { did: did, listRkey: listrkey != '-' ? listrkey : null, labelName: labelName != '-' ? labelName : null, mode: null, nickname: newName });
        var titleElement = listElement.querySelector('.list-metadata-row-name a');
        var segments = new URL(location.href).pathname.split('/');
        if (!titleElement && (segments[2] == 'labels' || segments[2] == 'lists')) titleElement =  document.querySelector('h1');
        if (titleElement) { 
            if (newName) {
                titleElement.innerHTML = '';
                var del = document.createElement('del');
                del.textContent = buttonElement.dataset.originalname;
                titleElement.appendChild(del);
                titleElement.appendChild(document.createTextNode(' ' + newName));
            } else { 
                titleElement.textContent = buttonElement.dataset.originalname;
            }
        }
    }
}

function formatTwoSignificantDigits(displayValue) { 
    var r = (Math.floor(displayValue * 10) / 10).toFixed(1);
    if (r.length > 3)
        r = Math.floor(displayValue) + '';
    return r;

}

function formatEngagementCount(value)
{
    if (value < 1_000)
    {
        // 1..999
        return value + '';
    }
    else if (value < 1_000_000)
    {
        // 1.0K..9.9K
        // 10K..999K
        return formatTwoSignificantDigits(value / 1_000.0) + "K";
    }
    else if (value < 1_000_000_000)
    {
        // 1.0M..9.9M
        // 10M..999M
        return formatTwoSignificantDigits(value / 1_000_000.0) + "M";
    }
    else
    {
        // 1.0B..9.9B
        // 10B..1234567B
        return formatTwoSignificantDigits(value / 1_000_000_000.0) + "B";
    }

    
}



async function updateLiveSubscriptions() {
    var visiblePosts = [...document.querySelectorAll('.post')].map(x => x.dataset.postdid + '/' + x.dataset.postrkey);
    var focalDid = document.querySelector('.post-list[data-focalpostdid]')?.dataset?.focalpostdid;
    if (!focalDid) focalDid = null;

    var visiblePostsSet = new Set(visiblePosts);
    var toSubscribe = visiblePosts.filter(x => !liveUpdatesPostIds.has(x));
    var toUnsubscribe = [...liveUpdatesPostIds].filter(x => !visiblePostsSet.has(x));
    liveUpdatesPostIds = visiblePostsSet;
    if (toSubscribe.length || toUnsubscribe.length) {
        await safeSignalrInvoke('SubscribeUnsubscribePosts', toSubscribe, toUnsubscribe);
    }

    var profilesToLoad = [...document.querySelectorAll(".profile-row[data-pendingload='1']")];
    if (profilesToLoad.length) { 
        for (const profile of profilesToLoad) {
            profile.dataset.pendingload = '0';
            var nodeId = crypto.randomUUID();
            profile.dataset.nodeid = nodeId;
            pendingProfileLoads.set(nodeId, new WeakRef(profile));
        }
        
        await safeSignalrInvoke('LoadPendingProfiles', profilesToLoad.map(x => ({ nodeId: x.dataset.nodeid, did: x.dataset.profiledid })))
    }

    
    var postsToLoad = [...document.querySelectorAll(".post[data-pendingload='1']")];
    if (postsToLoad.length) { 
        for (const post of postsToLoad) {
            post.dataset.pendingload = '0';
            var nodeId = crypto.randomUUID();
            post.dataset.nodeid = nodeId;
            pendingPostLoads.set(nodeId, new WeakRef(post));
        }
        
        var sideWithQuotee = new URL(location.href).pathname.endsWith('/quotes')
        await safeSignalrInvoke('LoadPendingPosts',
            postsToLoad.map(x => ({ nodeId: x.dataset.nodeid, did: x.dataset.postdid, rkey: x.dataset.postrkey, renderFlags: x.dataset.renderflags, repostedBy: x.dataset.repostedby, repostedByRkey: x.dataset.repostedbyrkey, replyChainLength: +x.dataset.replychainlength, fromFeedDid: x.dataset.fromfeeddid, fromFeedRkey: x.dataset.fromfeedrkey, isKnownContinuationOfPreviousPost: !!(+x.dataset.isknowncontinuationofpreviouspost) })),
            sideWithQuotee,
            focalDid
        )
    }

    var uncertainHandleDids = [...document.querySelectorAll('.handle-uncertain')].map(x => x.dataset.handledid );
    if (uncertainHandleDids.length) { 
        // necessary in case of race between EnrichAsync()/Signalr and DOM loading
        await safeSignalrInvoke('VerifyUncertainHandlesForDids', [...new Set(uncertainHandleDids)]);
    }
}

function countGraphemes(string) {
    const segmenter = new Intl.Segmenter('en', { granularity: 'grapheme' });
    const graphemes = Array.from(segmenter.segment(string));
    return graphemes.length;
  }
  

var MAX_GRAPHEMES = 300;
function composeTextAreaChanged() { 
    var textArea = document.querySelector('.compose-textarea');
    if (!textArea) return;
    var text = textArea.value;
    if (filesToUpload.length) { 
        var externalField = document.getElementById('compose-external-url-field');
        if (externalField.value) {
            // Files + External preview is not supported by bsky lexicon, so AppViewLite server will turn it into a facet link. Account for that.
            text += ' ' + externalField.value;
        }
    }
    var count = countGraphemes(text)
    var bar = document.querySelector('.compose-textarea-limit');
    bar.style.width = Math.min(1, count / MAX_GRAPHEMES) * 100 + '%';
    var exceeds = count > MAX_GRAPHEMES;
    bar.classList.toggle('compose-textarea-limit-exceeded', exceeds);
    document.querySelector('.compose-submit').disabled = exceeds;
}

function closeAutocompleteMenu() { 
    for (const autocomplete of document.querySelectorAll('.search-form-suggestions')) {
        autocomplete.classList.add('display-none');
        autocomplete.innerHTML = '';
    }

}

var lastAutocompleteDids = [];

async function searchBoxAutocomplete(forceResults) { 
    var searchbox = document.getElementById('search-box');
    var autocomplete = searchbox.parentElement.parentElement.querySelector('.search-form-suggestions');
    if (!autocomplete) return;
    if (new URL(location.href).searchParams.get('kind') == 'feeds') return;

    var text = searchbox.value;

    for (const tab of document.querySelectorAll('.tabbed-lists-header-inner a')) {
        var url = new URL(tab.href);
        var searchParams = url.searchParams;
        searchParams.delete('q');
        if (text) {
            searchParams.append('q', text);
        }

        tab.href = url.href;
    }
    
    if (!autocomplete.lastSearchToken) autocomplete.lastSearchToken = 0;
    var token = ++autocomplete.lastSearchToken;

    var result = text ? await httpGet('searchAutoComplete', forceResults ? { forceResults: lastAutocompleteDids.join(',') } : { q: text }) : { profiles: [] };
    if (autocomplete.lastSearchToken != token) return;
    var focusedIndex = [...autocomplete.children].findIndex(x => x == document.activeElement);
    autocomplete.innerHTML = result.html;
    if (focusedIndex != -1)
        autocomplete.children[focusedIndex]?.focus();
    lastAutocompleteDids = [...autocomplete.querySelectorAll('a[data-did]')].map(x => x.dataset.did)


    if (autocomplete.firstElementChild) autocomplete.classList.remove('display-none');
    else closeAutocompleteMenu(autocomplete)
}

document.addEventListener('play', e => {
    var video = e.target;
    if (video.tagName != 'VIDEO') return;
    var parentPost = video.closest('.post');
    if (parentPost)
        recordPostEngagement(parentPost, 'ViewedInTheaterOrWatchedVideo');
    if (video.didInstallHls) return;
    var playlistUrl = video.dataset.playlisturl;
    if (!playlistUrl) return;
    video.didInstallHls = true;
    if (new URL(playlistUrl, location.href).pathname.endsWith('.m3u8')) {
        var hls = new Hls();
        hls.loadSource(playlistUrl);
        hls.attachMedia(video);
    } else { 
        video.src = playlistUrl;
    }
}, true);


document.addEventListener('submit', e => {
    var target = e.target;
    if (target.classList.contains('compose-form')) {
        // can't have both @onsubmit and onsubmit in razor
        composeOnSubmit(target, e);
    }
}, true);

document.addEventListener('error', e => {
    var img = e.target;
    if (img.tagName == 'IMG' && img.classList.contains('post-image')) { 
        img.style.width = '48px';
        img.style.height = '48px';
    }
    if (img.tagName == 'IMG' && img.classList.contains('post-external-preview-image')) { 
        img.classList.add('display-none')
    }
}, true);


document.addEventListener('dragover', e => {
    var dropZone = document.getElementById('upload-drop-zone');
    if (dropZone) { 
        e.preventDefault();
        document.getElementById('upload-drop-zone').classList.add('dragover');
    }
});

document.addEventListener('dragleave', e => {
    var dropZone = document.getElementById('upload-drop-zone');
    if (dropZone) { 
        document.getElementById('upload-drop-zone').classList.remove('dragover');
    }
});

async function composeUploadChange(e) { 
    await composeAddFiles(e.target.files);
}

document.addEventListener('drop', async e => {
    var dropZone = document.getElementById('upload-drop-zone');
    if (dropZone) {
        e.preventDefault();
        dropZone.classList.remove('dragover');
        await composeAddFiles(e.dataTransfer.files);
    }
});
document.addEventListener('paste', (e) => {
    if (document.getElementById('upload-drop-zone')) {
        if (e.clipboardData.files.length > 0) {
            composeAddFiles(e.clipboardData.files);
        }
    }

    if (e.target.className == 'compose-textarea') {
        var text = (e.clipboardData || window.clipboardData).getData('text')?.trim();
        var externalField = document.getElementById('compose-external-url-field');
        if (text && (text.startsWith('http://') || text.startsWith('https://')) && !text.includes(' ') && !text.includes('\n') && !externalField.value) {
            
            try {
                var url = new URL(text);
            } catch { 
            }

            if (url) {
                if (url.host == 'bsky.app' || url.host == location.host) { 
                    var segments = url.pathname.split('/');
                    let did;
                    let rkey;
                    if (url.host == 'bsky.app') {
                        if (segments[1] == 'profile') {
                            if (segments[3] == 'post') {
                                did = segments[2];
                                rkey = segments[4];
                            } else if (!segments[3]) {
                                did = segments[2]
                                
                            }
                        }
                    } else { 
                        if (url.pathname.startsWith('/@')) { 
                            if (!segments[2]) {
                                did = segments[1].substring(1);
                            } else if(segments[2].length == 13 && /^\d/.test(segments[2])) { 
                                did = segments[1].substring(1);
                                rkey = segments[2];
                            }
                        }
                    }
                    const composeTextArea = document.querySelector('.compose-textarea');
                    if (did && rkey)
                    {
                        e.preventDefault();
                        const currentParams = new URL(location).searchParams;
                        fastNavigateTo('/compose?quoteDid=' + encodeURIComponent(did) + '&quoteRkey=' + encodeURIComponent(rkey) + (currentParams.get('replyDid') ? '&replyDid=' + encodeURIComponent(currentParams.get('replyDid')) + '&replyRkey=' + encodeURIComponent(currentParams.get('replyRkey')) : '') + (composeTextArea.value ? '&text=' + encodeURIComponent(composeTextArea.value) : ''), true);
                    } else if (did) { 
                        
                        e.preventDefault();
                        composeTextArea.value += (composeTextArea.value && !composeTextArea.value.endsWith(' ') && !composeTextArea.value.endsWith('\n') ? ' ' : '') + '@' + did;
                        composeTextAreaChanged();
                        
                    }
                    return;
                }

                if (!filesToUpload.length) {
                    
                    e.preventDefault();
                    const preview = document.querySelector('.compose-external-preview');
                    preview.classList.remove('display-none');
                    preview.querySelector('.post-external-preview').href = text;
                    preview.querySelector('.post-external-preview-domain-text').textContent = url.host.startsWith('www.') ? url.host.substring(4) : url.host;
                    preview.querySelector('.post-external-preview-title').textContent = '';
                    preview.querySelector('.post-external-preview-image').classList.add('display-none');
                    preview.querySelector('.post-external-preview-image').src = '';
                    preview.querySelector('.post-external-preview-summary').textContent = '';
                    externalField.value = url.href;
                    composeTextAreaChanged();

                    (async () => {
                        var response = await httpGet('GetOpenGraphPreview', { url: url.href });
                        preview.querySelector('.post-external-preview-title').textContent = response.title ?? url;
                        preview.querySelector('.post-external-preview-summary').textContent = response.description ?? '';
                        preview.querySelector('.post-external-preview-image').src = response.imageUrl;
                        preview.querySelector('.post-external-preview-image').classList.toggle('display-none', !response.imageUrl);
                    })();
                }
            }
        }
    }
});


async function hashArrayBuffer(buffer) {
    const hashBuffer = await crypto.subtle.digest('SHA-256', buffer);
    const hashArray = Array.from(new Uint8Array(hashBuffer));
    const hashHex = hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
    return hashHex;
}

/**@type {File[]}*/ var filesToUpload = [];
var didWarnFileFormat = false;

async function composeAddFile(/**@type {File}*/ file, altText) { 
    
    var nameParts = file.name.split('.');
    var ext = nameParts[nameParts.length - 1].toLowerCase();
    if (['jpg', 'jpeg', 'jpe', 'png', 'webp', 'jfif', 'gif', 'avif', 'bmp'].includes(ext)) {
        /* ok */
    } else if (['mp4', 'webm', 'mkv', 'mpg', 'mov', 'avi', 'wmv'].includes(ext)) {
        if (!didWarnFileFormat) alert('Video upload is not currently supported.');
        didWarnFileFormat = true;
        return;
    } else { 
        if (!didWarnFileFormat) alert('Unsupported file type.');
        didWarnFileFormat = true;
        return;
    }
    var arrayBuffer = await file.arrayBuffer();
    try {
        file.hashForDeduplication = await hashArrayBuffer(arrayBuffer);
        if (filesToUpload.some(x => x.hashForDeduplication == file.hashForDeduplication)) return;
    } catch(e) { 
        // only available in secure contexts
    }

    const externalLinkField = document.getElementById('compose-external-url-field');
    if (externalLinkField.value) { 
        const composeTextArea = document.querySelector('.compose-textarea')
        composeTextArea.value += (composeTextArea.value ? ' ' : '') + externalLinkField.value;
        composeRemoveExternalPreview();
    }

    filesToUpload.push(file);
    const li = document.createElement('li');
    li.classList.add('upload-file-entry')
    const blob = new Blob([arrayBuffer], { type: 'image/jpeg' });
    const url = URL.createObjectURL(blob);
    const img = document.createElement('img');
    img.src = url;
    file.blobUrlToRevoke = url;

    var thumbContainer = document.createElement('div');
    thumbContainer.classList.add('upload-thumb-container')
    var thumbLink = document.createElement('a');
    thumbLink.href = url;
    thumbLink.target = '_blank';
    thumbLink.appendChild(img);
    thumbContainer.appendChild(thumbLink);
    var removeButton = document.createElement('button');
    removeButton.textContent = '×';
    removeButton.addEventListener('click', e => {
        filesToUpload = filesToUpload.filter(x => x != file);
        li.remove();
        URL.revokeObjectURL(url);
        e.preventDefault();
        composeTextAreaChanged();
    });
    thumbContainer.appendChild(removeButton);
    var altTextArea = document.createElement('textarea');
    altTextArea.placeholder = 'Alt text'
    altTextArea.value = altText ?? '';
    altTextArea.maxLength = ALT_TEXT_MAX_GRAPHEMES; // actual grapheme check is done later
    file.altTextArea = altTextArea;
    li.appendChild(thumbContainer);
    li.appendChild(altTextArea)
    
    document.getElementById('upload-file-list').appendChild(li);
    composeTextAreaChanged();
}

function composeRemoveExternalPreview() { 
    var field = document.getElementById('compose-external-url-field');
    field.value = '';
    const preview = document.querySelector('.compose-external-preview');
    preview.classList.add('display-none');
    composeTextAreaChanged();
}

async function composeAddFiles(/**@type {FileList}*/ files) {
    didWarnFileFormat = false;
    for (let i = 0; i < files.length; i++) {
        if (filesToUpload.length >= 4) {
            alert('A post can include a maximum of 4 images.');
            break;
        }
        var file = files[i];
        await composeAddFile(file);
    }
}

var ALT_TEXT_MAX_GRAPHEMES = 2000;


async function composeOnSubmit(formElement, e) { 
    e.preventDefault();
    const formData = new FormData(formElement);
    formData.delete('Model.Files');
    formData.delete('Model.AltTexts');
    for (const file of filesToUpload) {
        formData.append('Model.Files', file);
        if (countGraphemes(file.altTextArea.value) > ALT_TEXT_MAX_GRAPHEMES)
            alert('Alt text is too long.')
        formData.append('Model.AltTexts', file.altTextArea.value);
    }

    try {
        const response = await fetch(location.href, {
            method: 'POST',
            body: formData
        });
        if (!response.ok) throw 'HTTP ' + response.status;
        var responseHtml = await response.text();
        var responseDom = parseHtmlAsWrapper(responseHtml);
        var error = responseDom.querySelector('.page-error');
        if (error) throw error.textContent.replace('❌', '').trim();
        console.log(responseHtml);
        var postUrl = response.url;
        if (!postUrl || postUrl == location.href) throw 'Could not create post.';
        clearFileUploads();
        await fastNavigateTo(postUrl);
        invalidateComposePage();
    } catch (e) { 
        alert(getDisplayTextForException(e));
    }
}

function clearFileUploads() { 
    for (const file of filesToUpload) {
        URL.revokeObjectURL(file.blobUrlToRevoke);
    }
    filesToUpload = [];
    document.getElementById('upload-file-list').innerHTML = '';
    composeTextAreaChanged();
}
function emojify(target = document.body) {
    twemoji.parse(target);
}


emojify();

const mutationObserver = new MutationObserver(mutations => {
    mutations.forEach(mutation => {
        if (mutation.addedNodes.length) {
            mutation.addedNodes.forEach(node => {
                if (node.nodeType === Node.ELEMENT_NODE) {
                    emojify(node);
                }
            });
        }
    });
});

mutationObserver.observe(document.body, { childList: true, subtree: true });
onInitialLoad();

