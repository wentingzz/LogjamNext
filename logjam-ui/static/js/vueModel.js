Vue.component('pie-chart', {
    props: ['label'],
    data: function() {
        return {
            config: {
                type: 'pie',
                data: {
                    datasets: [{
                        data: [ 77, 33 ],
                        backgroundColor: [
                            '#36a2eb',
                            '#ff6384',
                            '#cc65fe',
                            '#ffce56',
                            '#30c589',
                        ]
                    }],
                    labels: [
                        'Occurs',
                        'Does not occur',
                    ]
                },
                options: {
                    responsive: true,
                    title: {
                        display: true,
                        text: "Text occurrences across all nodes"
                    }
                }
            }
        };
    },
    template: '<canvas class="w-25"></canvas>',
    methods: {
        createChart(chartConfig) {
            const ctx = this.$el;
            const myChart = new Chart(ctx, chartConfig);
      }
    },

    mounted() {
        console.log(this.$el);
        this.createChart(this.config);
    }
})

function getOccurrences(logText, platform, sgVersion) {
    console.log(logText);
    console.log(platform);
    console.log(sgVersion);

}

var vm = new Vue({
    el: "#app",
    data: {
        platforms: [
            {text: "All Platforms", value: null},
            {text: "Platform A", value: "A"},
            {text: "Platform B", value: "B"},
            {text: "Platform C", value: "C"}],
        platform: null,
        versions: [
            {text: "All Versions", value: null},
            {text: "1.0", value: "1.0"},
            {text: "1.1.4", value: "1.1.4"},
            {text: "2.3", value: "2.3"}],
        sgVersion: null,
        logText: ""
    },
    methods: {
        getOccurrences: function (event) {
            // Currying!
            getOccurrences(this.logText, this.platform, this.sgVersion);
        }
    },
    delimiters: ['[[',']]']
})

