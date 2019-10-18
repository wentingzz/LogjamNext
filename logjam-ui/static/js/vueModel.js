Vue.component('pie-chart', {
    data: function() {
        return {
        };
    }
})

var vm = new Vue({
    el: "#app",
    data: {
        platforms: [
            {text: "All Platforms", value: "all"},
            {text: "Platform A", value: "A"},
            {text: "Platform B", value: "B"},
            {text: "Platform C", value: "C"}],
        platform: "all",
        encoding: "all",
        versions: [
            {text: "All Versions", value: "all"},
            {text: "1.0", value: "1.0"},
            {text: "1.1.4", value: "1.1.4"},
            {text: "2.3", value: "2.3"}],
        storageGridVersion: "all",
        logText: ""
    },
    delimiters: ['[[',']]']
})

