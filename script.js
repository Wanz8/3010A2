function setCookie(cname, cvalue, exdays) {
    var d = new Date();
    d.setTime(d.getTime() + (exdays * 24 * 60 * 60 * 1000));
    var expires = "expires="+d.toUTCString();
    document.cookie = cname + "=" + cvalue + ";" + expires + ";path=/";
}

function login() {
    var username = document.getElementById('username').value;
    if (!username) {
        alert("Please enter a username.");
        return false; // Prevent form submission
    }

    var xhr = new XMLHttpRequest();
    xhr.open('POST', '/api/login', true);
    xhr.setRequestHeader('Content-type', 'application/json');
    xhr.onload = function() {
        var response = JSON.parse(this.responseText);
        if (response.status === "Logged in!") {
            // 设置cookie
            setCookie("username", username, 1); // Set cookie when logged in

            // 隐藏登录页面，显示主页面
            document.getElementById('login-page').style.display = 'none';
            document.getElementById('main-page').style.display = 'block';

            // 显示注销按钮
            document.getElementById('logout-btn').style.display = 'block';

            // 获取并显示推文
            getTweets();
        } else {
            // 处理登录失败的情况
            alert("Login failed: " + response.status);
        }
    };

    var data = { username: username };
    xhr.send(JSON.stringify(data));
    return false; // 防止表单提交
}

var username;

function getParameterByName(name, url) {
    if (!url) url = window.location.href;
    name = name.replace(/[\[\]]/g, "\\$&");
    var regex = new RegExp("[?&]" + name + "(=([^&#]*)|&|#|$)"),
        results = regex.exec(url);
    if (!results) return null;
    if (!results[2]) return '';
    return decodeURIComponent(results[2].replace(/\+/g, " "));
}


function getTweets() {
    var xhr = new XMLHttpRequest();
    xhr.open('GET', '/api/tweet', true);
    xhr.onload = function() {
        if (this.status == 200) {
            var tweets = JSON.parse(this.responseText);
            var output = '';
            for (var i in tweets) {
                output += '<li>' +
                    tweets[i].content + ' by ' + tweets[i].username +
                    ' <button onclick="updateTweet(' + tweets[i].id + ')">Update</button>' +
                    ' <button onclick="deleteTweet(' + tweets[i].id + ')">Delete</button>' +
                    '</li>';
            }
            document.getElementById('tweets').innerHTML = output;
        }
    };
    xhr.send();
}

function postTweet() {
    var xhr = new XMLHttpRequest();
    xhr.open('POST', '/api/tweet', true);
    xhr.setRequestHeader('Content-type', 'application/json');
    xhr.onload = function() {
        if (this.status == 201) {
            getTweets(); // 重新加载推文
        }
    }
    var data = {
        content: document.getElementById('tweet').value,
        username: getCookie('username')
    };
    xhr.send(JSON.stringify(data));
}
function updateTweet(id) {
    var updatedContent = prompt("Update your tweet:", "");
    if (updatedContent !== null) {
        var xhr = new XMLHttpRequest();
        xhr.open('PUT', `/api/tweet/${id}`, true);
        xhr.setRequestHeader('Content-type', 'application/json');
        xhr.onload = function() {
            if (this.status == 200) {
                getTweets(); // refresh the displayed tweets after updating
            }
        }
        var data = {
            content: updatedContent,
            username: getCookie('username')
        };
        xhr.send(JSON.stringify(data));
    }
}
function deleteTweet(tweetId) {
    var xhr = new XMLHttpRequest();
    xhr.open('DELETE', `/api/tweet/${tweetId}`, true);
    xhr.onload = function() {
        if (this.status == 200) {
            console.log(this.responseText);
            getTweets(); // Refresh the tweets display after successful deletion
        }
    };
    xhr.send();
}

function logout() {
    // 添加注销的XHR请求
    var xhr = new XMLHttpRequest();
    xhr.open('DELETE', '/api/login', true);
    xhr.onload = function() {
        if (this.status == 200) {
            // 注销成功，清除页面内容，并显示登录界面
            setCookie("username", "", -1); // 清除cookie
            document.getElementById('main-page').style.display = 'none';
            document.getElementById('login-page').style.display = 'block';
            document.getElementById('logout-btn').style.display = 'block';
        }
    };
    xhr.send();
}

function getCookie(name) {
    var value = "; " + document.cookie;
    var parts = value.split("; " + name + "=");
    if (parts.length == 2) return parts.pop().split(";").shift();
}



window.onload = function() {
    username = getCookie('username');
    if (username) {
        // 用户已经登录，显示主页面和注销按钮
        document.getElementById('login-page').style.display = 'none';
        document.getElementById('main-page').style.display = 'block';
        document.getElementById('logout-btn').style.display = 'block';
        getTweets();
    } else {
        // 用户未登录，显示登录页面并隐藏主页面和注销按钮
        document.getElementById('login-page').style.display = 'block';
        document.getElementById('main-page').style.display = 'none';
        document.getElementById('logout-btn').style.display = 'none';
    }
};

