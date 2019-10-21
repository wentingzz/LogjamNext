Vue.use(VueResource);

Vue.component('pie-chart', {
    props: {
        title: String,
        labels: Array,
        values: Array
    },
    data: function() {
        return {
            config: {
                type: 'pie',
                data: {
                    datasets: [{
                        data: [],
                        backgroundColor: [
                            '#36a2eb',
                            '#ff6384',
                            '#cc65fe',
                            '#ffce56',
                            '#30c589',
                        ]
                    }],
                    labels: []
                },
                options: {
                    responsive: true,
                    aspectRatio: 1,
                    title: {
                        display: true,
                        text: ""
                    }
                }
            }
        };
    },
    template: '<canvas></canvas>',
    methods: {
        createChart(chartConfig) {
            const ctx = this.$el;
            chartConfig.options.title.text = this.title;
            chartConfig.data.labels = this.labels;
            chartConfig.data.datasets[0].data = this.values;

            const myChart = new Chart(ctx, chartConfig);
      }
    },

    mounted() {
        console.log(this.$el);
        this.createChart(this.config);
    }
})


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
        logText: "",
        charts: {}
    },
    methods: {
        getOccurrences(event) {
            this.$http.post('/occurrences', {logText: this.logText}).then( response => {
                this.charts = response.body;
            }, response => {
                alert("Error getting occurrences: " + response.status + "\n" + response.json());
            });
        }
    },
    delimiters: ['[[',']]']
})

