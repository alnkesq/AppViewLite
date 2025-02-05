var hasBlazor = !!window.Blazor;


var liveUpdatesPostIds = new Set();
var notificationCount = parseInt(document.querySelector('.notification-badge')?.textContent ?? 0);

var historyStack = [];

/** @type {Map<string, WeakRef<HTMLElement>>} */
var pendingProfileLoads = new Map();
/** @type {Map<string, WeakRef<HTMLElement>>} */
var pendingPostLoads = new Map();

function updatePageTitle() {
    document.title = notificationCount ? '(' + notificationCount + ') ' + appliedPageObj.title : appliedPageObj.title;
    var badge = document.querySelector('.notification-badge');
    if (badge) {
        badge.textContent = notificationCount;
        badge.classList.toggle('display-none', notificationCount == 0);
    }
}


function parseHtmlAsElement(html) { 
    var temp = document.createElement('div');
    temp.innerHTML = html;
    return temp.firstElementChild;
}

var liveUpdatesConnection = null;
var liveUpdatesConnectionFuture = (async () => {


    var connection = new signalR.HubConnectionBuilder().withUrl("/api/live-updates").withAutomaticReconnect().build();
    connection.on('PostEngagementChanged', (stats, ownRelationship) => {
        //console.log('PostEngagementChanged: ');
        for (const postElement of document.querySelectorAll('.post[data-postrkey="' + stats.rKey + '"][data-postdid="' + stats.did + '"]')) {
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
        oldnode.replaceWith(parseHtmlAsElement(html));
    });
    connection.on('PostRendered', (nodeid, html) => { 
        var oldnoderef = pendingPostLoads.get(nodeid);
        var oldnode = oldnoderef?.deref();
        if (!oldnode) { 
            if (oldnoderef) pendingPostLoads.delete(nodeid);
            return;
        }
        oldnode.replaceWith(parseHtmlAsElement(html));
        //if (newnode.querySelector(".post-quoted[data-pendingload='1']"))

        updateLiveSubscriptions();
    });
    connection.on('HandleVerificationResult', (did, handle) => {
        for (const a of document.querySelectorAll('.handle-generic[data-handledid="' + did + '"]')) {
            a.textContent = handle;
            a.classList.remove('handle-uncertain');
            a.classList.toggle('handle-invalid', handle == 'handle.invalid');
        }
        for (const a of document.querySelectorAll('.profile-badge-pending[data-badgedid="' + did + '"][data-badgehandle="'+ handle.toLowerCase() +'"]')) {
            a.classList.remove('profile-badge-pending');
        }
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

    if (!hasBlazor) { 
        document.querySelector('[autofocus]')?.focus();
    }

    var seenNotificationId = document.querySelector('#notification-newest-id')?.dataset['newestnotification'];
    if (seenNotificationId) {
        var token = applyPageId;
        setTimeout(() => {
            if (applyPageId != token) return;
            document.querySelectorAll('.notification-new').forEach(x => x.classList.remove('notification-new'));
            httpPost('MarkLastSeenNotification', {
                notificationId: seenNotificationId
            });
        }, 700);
    }
    
    updateSidebarButtonScrollVisibility();

    var searchbox = document.querySelector('.search-form-query input');
    var query = null;
    if (searchbox) { 
        var query = searchbox.value;
        searchbox.setSelectionRange(query.length, query.length);
    }
    
    updateBottomBarSelectedTab();
}

function updateBottomBarSelectedTab() { 
    var url = new URL(window.location.href);
    for (const a of document.querySelectorAll('.bottom-bar a')) {
        var path = new URL(a.href).pathname
        var selected = path == url.pathname
        a.firstElementChild.classList.toggle('display-none', selected)
        a.lastElementChild.classList.toggle('display-none', !selected)
    }
}

if (hasBlazor) {
    // https://github.com/dotnet/aspnetcore/issues/51338#issuecomment-1766578689
    Blazor.addEventListener('enhancedload', () => {
        applyPageFocus();
    });
}


function fastNavigateIfLink(event) { 
    var url = null;
    var t = event.target;
    var a = null;

    if (t instanceof HTMLElement && t.classList.contains('post-body-link-to-thread')) { 
        var href = t.querySelector('.post-body-link-to-thread-text')?.href
        if (href) {
            fastNavigateTo(href);
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
    } else if (!hasBlazor && !a.target) { 
        fastNavigateTo(url.href);
        event.preventDefault();
    }
    return true;
}


/**@type {href: string, dateFetched: number, dom: HTMLElement, title: string, scrollTop: number}[] */
var recentPages = [];
var applyPageId = 0;

var appliedPageObj = null;

applyPageFocus();

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

    var oldMainWasHidden = false;
    setTimeout(() => {
        if (token != applyPageId) return;
        if (oldMainWasHidden) return;

        oldMainWasHidden = true;
        appliedPageObj.scrollTop = document.scrollingElement.scrollTop;
        oldMain.classList.add('display-none');
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
    if (p.dom != oldMain) {
        if (!oldMainWasHidden)
            appliedPageObj.scrollTop = document.scrollingElement.scrollTop;
        oldMain.remove();
        page.appendChild(p.dom);
    }
    
    oldMainWasHidden = true;

    appliedPageObj = p;
    updatePageTitle();
    
    if (scrollToTop) {
        applyPageFocus();
    } else { 
        document.scrollingElement.scrollTop = p.scrollTop;
    }
    updateLiveSubscriptions();


}

async function fetchOrReusePageAsync(href, token) { 
    
    var p = recentPages.filter(x => x.href == href)[0];
    if (p) {
        return p;
    } else { 
        var response = await fetch(href);
        if (response.status != 200) { 
            throw ('HTTP ' + response.status);
        }
        var temp = document.createElement('div');
        temp.innerHTML = await response.text();
        if (token != applyPageId) throw 'Superseded navigation.'
        var dom = temp.querySelector('main');
        var title = temp.querySelector('title').textContent;
        var p = { href: href, dom: dom, dateFetched: Date.now(), title: title, scrollTop: 0 };
        recentPages.push(p)
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

function updateSidebarButtonScrollVisibility() { 
    var scrollTop = document.documentElement.scrollTop;
    var scrollDelta = scrollTop - prevScrollTop;
    if (Math.abs(scrollDelta) >= 3) {
        document.querySelector('.sidebar-button').classList.toggle('sidebar-button-fixed', scrollDelta < 0);
        prevScrollTop = scrollTop;
    }
    document.querySelector('.scroll-up-button').classList.toggle('display-none', scrollTop < 700);
}
if (!hasBlazor) {
    window.addEventListener('popstate', e => {
        var popped = historyStack.pop();
        if (popped != location.href) { 
            console.log("History stack (" + popped +") / pushState (" + location.href + ") mismatch");
        }
        applyPage(location.href, false);
    });
    
    appliedPageObj = {
        href: location.href,
        dateFetched: Date.now(),
        dom: document.querySelector('main'),
        title: document.title,
        scrollTop: document.scrollingElement.scrollTop
    };
    recentPages.push(appliedPageObj);
    
    window.addEventListener('scroll', async e => {
        updateSidebarButtonScrollVisibility();
        var scrollingElement = document.scrollingElement;
        var scrollTop = scrollingElement.scrollTop
        if (scrollTop <= 0) return;
        var scrollTopMax = scrollingElement.scrollHeight - scrollingElement.clientHeight;
        var remainingToBottom = scrollTopMax - scrollTop;
        if (remainingToBottom >= 500) return;
        var paginationButton = document.querySelector('.pagination-button');
        if (!paginationButton) return;
        if (paginationButton.querySelector('.spinner')) return;
        var oldList = document.querySelector('.main-paginated-list');


        paginationButton.classList.add('spinner-visible')
        paginationButton.insertAdjacentHTML('beforeend', '<div class="spinner"><svg height="100%" viewBox="0 0 32 32" width="100%"><circle cx="16" cy="16" fill="none" r="14" stroke-width="4" style="stroke: rgb(25, 118, 210); opacity: 0.2;"></circle><circle cx="16" cy="16" fill="none" r="14" stroke-width="4" style="stroke: rgb(25, 118, 210); stroke-dasharray: 80px; stroke-dashoffset: 60px;"></circle></svg></div>')
        var nextPage = await fetch(paginationButton.querySelector('a').href);
        if (nextPage.status != 200) throw ('HTTP ' + nextPage.status);
        var temp = document.createElement('div');
        temp.innerHTML = await nextPage.text();


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

        if (anyChildren) updateLiveSubscriptions();
    });

    document.addEventListener('keydown', e => {
        if (e.key == 'Escape') { 
            closeCurrentMenu();
            closeAutocompleteMenu();

            e.preventDefault();
        }
    });

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
                    }
                }
            }
        }

        var autocomplete = target.closest('.search-form-suggestions');
        if (!autocomplete)
            closeAutocompleteMenu();
    });


    updateLiveSubscriptions();
    updatePageTitle();
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
        method: 'POST'
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
        }
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
        getOrCreateRepostToggler(did, rkey, postElement).toggleIfNotBusyAsync();
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
            postsToLoad.map(x => ({ nodeId: x.dataset.nodeid, did: x.dataset.postdid, rkey: x.dataset.postrkey, renderflags: x.dataset.renderflags })),
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
    var text = document.querySelector('.compose-textarea').value;
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
    var text = searchbox.value;
    
    if (!autocomplete.lastSearchToken) autocomplete.lastSearchToken = 0;
    var token = ++autocomplete.lastSearchToken;

    var result = text ? await httpGet('searchAutoComplete', forceResults ? { forceResults: lastAutocompleteDids.join(',') } : { q: text }) : { profiles: [] };
    if (autocomplete.lastSearchToken != token) return;
    autocomplete.innerHTML = result.html;
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

