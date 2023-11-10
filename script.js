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
        return false;
    }

    var xhr = new XMLHttpRequest();
    xhr.open('POST', '/api/login', true);
    xhr.setRequestHeader('Content-type', 'application/json');
    xhr.onload = function() {
        var response = JSON.parse(this.responseText);
        if (response.status === "Logged in!") {

            setCookie("username", username, 1); // Set cookie when logged in


            document.getElementById('login-page').style.display = 'none';
            document.getElementById('main-page').style.display = 'block';
            document.getElementById('logout-btn').style.display = 'block';

            getTweets();
        } else {
            alert("Login failed: " + response.status);
        }
    };

    var data = { username: username };
    xhr.send(JSON.stringify(data));
    return false;
}

var username;



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
            getTweets();
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
    var xhr = new XMLHttpRequest();
    xhr.open('DELETE', '/api/login', true);
    xhr.onload = function() {
        if (this.status == 200) {
            setCookie("username", "", -1);
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
        document.getElementById('login-page').style.display = 'none';
        document.getElementById('main-page').style.display = 'block';
        document.getElementById('logout-btn').style.display = 'block';
        getTweets();
    } else {
        document.getElementById('login-page').style.display = 'block';
        document.getElementById('main-page').style.display = 'none';
        document.getElementById('logout-btn').style.display = 'none';
    }
};

