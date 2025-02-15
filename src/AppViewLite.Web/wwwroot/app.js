


var liveUpdatesPostIds = new Set();
var pageTitleOverride = null;
var notificationCount = parseInt(document.querySelector('.sidebar .notification-badge')?.textContent ?? 0);

var currentFeedHasNewPosts = false;
var currentFeedHasNewPostsDelay = -1;
var currentFeedHasNewPostsTimeout = null;

var theaterReturnUrl = null;


var historyStack = [];

/** @type {Map<string, WeakRef<HTMLElement>>} */
var pendingProfileLoads = new Map();
/** @type {Map<string, WeakRef<HTMLElement>>} */
var pendingPostLoads = new Map();

function updatePageTitle() {
    document.title = (notificationCount ? '(' + notificationCount + ') ' : '') + (pageTitleOverride ?? appliedPageObj.title);
    var badge = document.querySelector('.sidebar .notification-badge');
    if (badge) {
        badge.textContent = notificationCount;
        badge.classList.toggle('display-none', notificationCount == 0);
    }
    var badge = document.querySelector('.bottom-bar .notification-badge');
    if (badge) {
        badge.textContent = notificationCount;
        badge.classList.toggle('display-none', notificationCount == 0);
    }
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
        for (const a of document.querySelectorAll('.profile-badge-pending[data-badgedid="' + did + '"][data-badgehandle="' + handle.toLowerCase() + '"]')) {
            a.classList.remove('profile-badge-pending');
        }
    }
}

var liveUpdatesConnection = null;
var liveUpdatesConnectionFuture = (async () => {


    var connection = new signalR.HubConnectionBuilder().withUrl("/api/live-updates").withAutomaticReconnect().build();
    connection.on('PostEngagementChanged', (stats, ownRelationship) => {
        //console.log('PostEngagementChanged: ');
        for (const postElement of document.querySelectorAll(getPostSelector(stats.did, stats.rKey))) {
            var likeToggler = getOrCreateLikeToggler(stats.did, stats.rKey, postElement);
            likeToggler.applyLiveUpdate(stats.likeCount, ownRelationship?.likeRkey);

            postElement.dataset.quotecount = stats.quoteCount;

            var repostToggler = getOrCreateRepostToggler(stats.did, stats.rKey, postElement);
            repostToggler.applyLiveUpdate(stats.repostCount, ownRelationship?.repostRkey);
            
            setPostStats(postElement, stats.replyCount, 'replies', 'reply', 'replies');
        }
    });
    connection.on('NotificationCount', (count) => {
        notificationCount = count;
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
    liveUpdatesConnection = connection;

    await connection.start();
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


function applyPageFocus() {
    var focalPost = document.querySelector('.post-focal');
    if (focalPost && document.querySelector('.post') != focalPost) focalPost.scrollIntoView();
    else window.scrollTo({ top: 0, left: 0, behavior: 'instant' });

    
    document.querySelector('[autofocus]')?.focus();
    

    var seenNotificationId = document.querySelector('#notification-newest-id')?.dataset['newestnotification'];
    if (seenNotificationId) {
        var token = applyPageId;
        setTimeout(() => {
            if (applyPageId != token) return;
            document.querySelectorAll('.notification-new').forEach(x => x.classList.remove('notification-new'));
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

    if (t instanceof HTMLElement && t.classList.contains('post-body') && !t.parentElement.classList.contains('post-focal')) { 
        var backgroundLink = t.parentElement.querySelector('.post-background-link');
        if (backgroundLink.href && !userSelectedTextSinceLastMouseDown) {
            if (backgroundLink.target == '_blank') window.open(backgroundLink.href);
            else fastNavigateTo(backgroundLink.href);
            return true;
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

    if (a.parentElement?.classList?.contains('pagination-button')) { 
        event.preventDefault();
        loadNextPage(true);
        return true;
    }

    if (a.classList.contains('post-image-for-threater')) { 
        var index = [...a.parentElement.children].indexOf(a) + 1;
        var post = a.closest('.post');
        theaterReturnUrl = location.href;
        fastNavigateTo('/@' + post.dataset.postdid + '/' + post.dataset.postrkey + '/media/' + index, false, false);
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

    if ((document.querySelector('#components-reconnect-modal') || ((url.pathname == '/login' || url.pathname == '/logout') && url.host == window.location.host))) {
        window.location = url;
    } else if (!a.target) { 
        fastNavigateTo(url.href);
        event.preventDefault();
    }
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



    var theaterForPostInfo = tryTrimMediaSegments(location.href);
    var theaterForPostElement = theaterForPostInfo ? document.querySelector(getPostSelector(theaterForPostInfo.postdid, theaterForPostInfo.postrkey)) : null;
    if (theaterForPostElement) {
        var body = theaterForPostElement.querySelector('.post-body')?.textContent;
        pageTitleOverride = theaterForPostElement.querySelector('.post-author').textContent + (body ? ': ' + body : '');
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
       document.scrollingElement.scrollTop = p.scrollTop;
    }

}

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
        
        var postElement = document.querySelector(getPostSelector(theaterInfo.postdid, theaterInfo.postrkey));
        var includePostText = theaterReturnUrl && (theaterReturnUrl.includes('?media=1') || theaterReturnUrl.includes('kind=media'));

        var images = [...postElement.children].filter(x => x.classList.contains('post-image-list'))[0].children;
        var postText = includePostText ? [...postElement.children].filter(x => x.classList.contains('post-body'))[0]?.textContent : null;
        
        var a = images[theaterInfo.mediaId - 1]
        document.querySelector('.theater-image').src = ''; // ensure old image is never displayed
        document.querySelector('.theater-image').src = a.href;
        var alt = a.title;
        var description = alt && postText ? postText + "\n\nImage description:\n" + alt : (alt || postText);

        document.querySelector('.theater-alt').textContent = description;
        document.querySelector('.theater-alt').classList.toggle('display-none', !description);
        document.querySelector('.theater-alt').classList.toggle('theater-alt-reduced-max-height', includePostText);
        document.querySelector('.theater-full-post-link').classList.toggle('display-none', !includePostText);
        document.querySelector('.theater-full-post-link').href = theaterInfo.href;
    }

    maybeLoadNextPage();
}

function tryTrimMediaSegments(href) { 
    var url = new URL(href);
    var segments = url.pathname.split('/');
    if (segments[3] == 'media') { 
        return {
            href: new URL(url.origin + '/' + segments[1] + '/' + segments[2] + url.search).href,
            postdid: segments[1].substring(1),
            postrkey: segments[2],
            mediaId: +segments[4]
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
        var response = await fetch(href, { signal: AbortSignal.timeout(20000) });
        if (response.status != 200) { 
            throw ('HTTP ' + response.status);
        }
        var temp = parseHtmlAsWrapper(await response.text());
        if (token != applyPageId) throw 'Superseded navigation.'
        var dom = temp.querySelector('main');
        var title = temp.querySelector('title').textContent;
        var p = { href: href, dom: dom, dateFetched: Date.now(), title: title, scrollTop: 0 };
        recentPages.push(p)
        while (recentPages.length > 7)
            recentPages.splice(0, 1);
        return p;
    }
}

function fastNavigateTo(href, preferRefresh = null, scrollToTop = null) { 
    if (href.startsWith('/')) href = location.origin + href;
    if (!href.startsWith(window.location.origin + '/')) { 
        window.location.href = href;
        return;
    }

    if (window.location.href == href) {
        window.scrollTo(0, 0);
    } else {

        if (href == historyStack[historyStack.length - 1]) { 
            history.back();
            return;
        }
        historyStack.push(location.href);
        window.history.pushState(null, null, href);
    }
    applyPage(href, preferRefresh, scrollToTop);
}


var currentlyOpenMenu = null;
var currentlyOpenMenuButton = null;
function closeCurrentMenu() { 
    if (!currentlyOpenMenu) return;
    currentlyOpenMenu.classList.remove('menu-visible')
    currentlyOpenMenu = null;
    currentlyOpenMenuButton = null;
}
var prevScrollTop = 0;

async function checkUpdatesForCurrentFeed() { 
    console.log('Checking updates for the current feed')
    var token = applyPageId;
    var url = new URL(location.href);
    url.searchParams.delete('limit');
    url.searchParams.append('limit', 1);
    var response = await fetch(url.href);
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
    var showScrollUp = scrollTop >= 700;
    document.querySelector('.scroll-up-button').classList.toggle('display-none', !showScrollUp);

    var path = new URL(location.href).pathname;

    var needsScrollUpTimer =
        !currentFeedHasNewPosts &&
        showScrollUp &&
        (path == '/following' || path.startsWith('/feed/') || path == '/firehose');
    
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
    var imageCount = document.querySelector(getPostSelector(theater.postdid, theater.postrkey) + ' .post-image-list').children.length;
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
    fastNavigateTo('/@' + theater.postdid + '/' + theater.postrkey + '/media/' + mediaId, false, false);
    theaterReturnUrl = prevReturn;
    return true;
}

var SPINNER_HTML = '<div class="spinner"><svg height="100%" viewBox="0 0 32 32" width="100%"><circle cx="16" cy="16" fill="none" r="14" stroke-width="4" style="stroke: rgb(25, 118, 210); opacity: 0.2;"></circle><circle cx="16" cy="16" fill="none" r="14" stroke-width="4" style="stroke: rgb(25, 118, 210); stroke-dasharray: 80px; stroke-dashoffset: 60px;"></circle></svg></div>';

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
        var nextPage = await fetch(paginationButton.querySelector('a').href, { signal: AbortSignal.timeout(20000) });
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
        errorDetails.textContent = e || 'Unknown error.';
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

function maybeLoadNextPage() { 
    var scrollingElement = document.scrollingElement;
    var scrollTop = scrollingElement.scrollTop
    var scrollTopMax = scrollingElement.scrollHeight - scrollingElement.clientHeight;
    var remainingToBottom = scrollTopMax - scrollTop;
    if (remainingToBottom >= 500) return;
    loadNextPage(false);
}

function onInitialLoad() {
    window.addEventListener('popstate', e => {
        var popped = historyStack.pop();
        if (popped != location.href) { 
            console.log("History stack (" + popped +") / pushState (" + location.href + ") mismatch");
        }
        applyPage(location.href, false);
    });
    
    appliedPageObj = {
        href: trimMediaSegments(location.href),
        dateFetched: Date.now(),
        dom: document.querySelector('main'),
        title: document.title,
        scrollTop: document.scrollingElement.scrollTop
    };
    recentPages.push(appliedPageObj);
    
    window.addEventListener('scroll', e => {
        updateSidebarButtonScrollVisibility();
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
                if (e.key == 'ArrowUp') {
                    
                    if (currentMenuItem.previousElementSibling) {
                        currentMenuItem.previousElementSibling.focus();
                    } else {
                        if (currentMenuItem.classList.contains('search-form-suggestion')) document.querySelector('#search-box').focus();
                        else currentMenuItem.parentElement.lastElementChild.focus();
                    }
                    e.preventDefault();
                    return;
                } else if (e.key == 'ArrowDown') {
                    if (currentMenuItem.nextElementSibling) {
                        currentMenuItem.nextElementSibling.focus();
                    } else {
                        currentMenuItem.parentElement.firstElementChild.focus();
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
            menuItem?.focus();
        }
    });

    document.addEventListener('mousedown', e => { 
        userSelectedTextSinceLastMouseDown = false;
        if (e.button == 1) { 
            if (e.target?.classList?.contains('post-body')) { 
                var href = e.target.parentElement.querySelector('.post-background-link')?.href;
                if (href) {
                    window.open(href);
                    e.preventDefault();
                }
            }
        }
    })

    document.addEventListener('click', e => {
        
        var target = e.target;

        if (currentlyOpenMenu) { 
            if (currentlyOpenMenuButton.contains(target)) { 
                closeCurrentMenu();
                return;
            }
        }

        if (fastNavigateIfLink(e))
            return;


        var actionButton = target.closest('.post-action-bar-button,[actionkind]');
        closeCurrentMenu();
        if (actionButton) { 
            var actionKind = actionButton.getAttribute('actionkind');
            if (actionKind) {
                console.log(actionKind);

                var postAction = postActions[actionKind];
                if (postAction) {
                    postAction.call(postActions,
                        getAncestorData(actionButton, 'postdid'),
                        getAncestorData(actionButton, 'postrkey'),
                        actionButton.closest('[data-postrkey]')
                    );
                    return;
                }
                
                var userAction = userActions[actionKind];
                if (userAction) {
                    userAction.call(postActions,
                        getAncestorData(actionButton, 'profiledid'),
                        getAncestorData(actionButton, 'followrkey'),
                        getAncestorData(actionButton, 'followsyou'),
                        actionButton.closest('[data-profiledid]')
                    );
                    return;
                }
            } else {
                if (actionButton == currentlyOpenMenuButton) closeCurrentMenu();
                else {
                    var prevMenu = actionButton.previousElementSibling;
                    if (prevMenu && prevMenu.classList.contains('menu')) {
                        prevMenu.classList.add('menu-visible');
                        
                        currentlyOpenMenuButton = actionButton;
                        currentlyOpenMenu = prevMenu;
                        currentlyOpenMenu.querySelector('a, button')?.focus();
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
            console.error(e);
        } finally { 
            this.busy = false;
        }
    }
}



async function httpPost(method, args) { 
    var response = await fetch('/api/' + method, {
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
    var response = await fetch('/api/' + method + '?' + new URLSearchParams(args).toString(), {
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
        async () => (await httpPost('CreatePostLike', { did, rkey })).rkey,
        async (rkey) => (await httpPost('DeletePostLike', { rkey })),
        (count, have) => { 
            var key = formatEngagementCount(count) + have.toString();
            if (key == prevKey) return;
            prevKey = key;
            setPostStats(postElement, count, 'likes', 'like', 'likes');
            setActionStats(postElement, count, 'like');
            postElement.querySelector('.post-action-bar-button-like').classList.toggle('post-action-bar-button-checked', have);
        });
}

function getOrCreateRepostToggler(did, rkey, postElement) { 
    var prevKey = '';
    return postElement.repostToggler ??= new ActionStateToggler(
        +postElement.dataset.repostcount,
        postElement.dataset.repostrkey,
        async () => (await httpPost('CreateRepost', { did, rkey })).rkey,
        async (rkey) => (await httpPost('DeleteRepost', { rkey })),
        (count, have) => { 
            var quoteCount = +postElement.dataset.quotecount;
            var key = formatEngagementCount(count) + '/' + formatEngagementCount(quoteCount) + '/' + formatEngagementCount(quoteCount + count) + have.toString();
            if (key == prevKey) return;
            prevKey = key;
            setPostStats(postElement, count, 'reposts', 'repost', 'reposts');
            setPostStats(postElement, quoteCount, 'quotes', 'quote', 'quotes');
            setActionStats(postElement, count + quoteCount, 'repost');
            postElement.querySelector('.post-action-bar-button-repost').classList.toggle('post-action-bar-button-checked', have);
            postElement.querySelector('.post-toggle-repost-menu-item').textContent = have ? 'Undo repost' : 'Repost'
        });
}

var postActions = {
    toggleLike: async function (did, rkey, postElement) { 
        getOrCreateLikeToggler(did, rkey, postElement).toggleIfNotBusyAsync();
    },
    toggleRepost: async function (did, rkey, postElement) { 
        if (did.startsWith('did:plc:') || did.startsWith('did:web:')) {
            getOrCreateRepostToggler(did, rkey, postElement).toggleIfNotBusyAsync();
        } else { 
            var children = [...postElement.children];
            var originalText = children.filter(x => x.classList.contains('post-body'))[0]?.textContent?.trim();
            var url = children.filter(x => x.classList.contains('post-background-link'))[0]?.href;
            var text = (originalText ? "\"" + originalText + "\"\n" : "\n") + (url ? url : '')
            fastNavigateTo('/compose?text=' + encodeURIComponent(text.trim()));
        }
        
    },
    composeReply: async function (did, rkey) { 
        fastNavigateTo(`/compose?replyDid=${did}&replyRkey=${rkey}`)
    },
    composeQuote: async function (did, rkey) { 
        fastNavigateTo(`/compose?quoteDid=${did}&quoteRkey=${rkey}`)
    },
    viewOnBluesky: async function (did, rkey) { 
        window.open(`https://bsky.app/profile/${did}/post/${rkey}`);
    },
    deletePost: async function (did, rkey, node) { 
        await httpPost('DeletePost', { rkey });
        var nextSeparator = node.nextElementSibling;
        if (nextSeparator?.classList.contains('post-group-separator')) nextSeparator.remove();
        node.remove();
    },
    copyPostUrl: async function (did, rkey) { 
        navigator.clipboard.writeText(location.origin + '/@' + did + '/' + rkey)
    },
    translatePost: async function (did, rkey, postElement) { 
        var text = postElement.querySelector('.post-body').textContent
        window.open('https://translate.google.com/?sl=auto&tl=en&text=' + encodeURIComponent(text) + '&op=translate');
    }
}

var userActions = {
    toggleFollow: async function (profiledid, followrkey, followsyou, postElement) { 
        postElement.followToggler ??= new ActionStateToggler(
            0,
            followrkey,
            async () => (await httpPost('CreateFollow', { did: profiledid })).rkey,
            async (rkey) => (await httpPost('DeleteFollow', { rkey })),
            (count, have) => { 
            var btn = postElement;
            btn.textContent = have ? 'Unfollow' : +followsyou ? 'Follow back' : 'Follow';
            btn.classList.toggle('follow-button-unfollow', have);
        });
        postElement.followToggler.toggleIfNotBusyAsync();
    },
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
        // 19K..999K
        return formatTwoSignificantDigits(value / 1_000.0) + "K";
    }
    else
    {
        // 1.0M..9.9M
        // 10M..1234567M
        return formatTwoSignificantDigits(value / 1_000_000.0) + "M";
    }

    
}



async function updateLiveSubscriptions() {
    var connection = await liveUpdatesConnectionFuture;
    var visiblePosts = [...document.querySelectorAll('.post')].map(x => x.dataset.postdid + '/' + x.dataset.postrkey);
    var focalDid = document.querySelector('.post-list[data-focalpostdid]')?.dataset?.focalpostdid;
    if (!focalDid) focalDid = null;

    var visiblePostsSet = new Set(visiblePosts);
    var toSubscribe = visiblePosts.filter(x => !liveUpdatesPostIds.has(x));
    var toUnsubscribe = [...liveUpdatesPostIds].filter(x => !visiblePostsSet.has(x));
    liveUpdatesPostIds = visiblePostsSet;
    if (toSubscribe.length || toUnsubscribe.length) {
        await connection.invoke('SubscribeUnsubscribePosts', toSubscribe, toUnsubscribe);
    }

    var profilesToLoad = [...document.querySelectorAll(".profile-row[data-pendingload='1']")];
    if (profilesToLoad.length) { 
        for (const profile of profilesToLoad) {
            profile.dataset.pendingload = '0';
            var nodeId = crypto.randomUUID();
            profile.dataset.nodeid = nodeId;
            pendingProfileLoads.set(nodeId, new WeakRef(profile));
        }
        
        await connection.invoke('LoadPendingProfiles', profilesToLoad.map(x => ({ nodeId: x.dataset.nodeid, did: x.dataset.profiledid })))
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
        await connection.invoke('LoadPendingPosts',
            postsToLoad.map(x => ({ nodeId: x.dataset.nodeid, did: x.dataset.postdid, rkey: x.dataset.postrkey, renderFlags: x.dataset.renderflags, repostedBy: x.dataset.repostedby })),
            sideWithQuotee,
            focalDid
        )
    }

    var uncertainHandleDids = [...document.querySelectorAll('.handle-uncertain')].map(x => x.dataset.handledid );
    if (uncertainHandleDids.length) { 
        // necessary in case of race between EnrichAsync()/Signalr and DOM loading
        await connection.invoke('VerifyUncertainHandlesForDids', [...new Set(uncertainHandleDids)]);
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




function emojify(target = document.body) {
    twemoji.parse(target);
}


emojify();

const observer = new MutationObserver(mutations => {
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

observer.observe(document.body, { childList: true, subtree: true });
onInitialLoad();

