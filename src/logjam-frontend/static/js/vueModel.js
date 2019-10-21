Vue.use(VueResource);

function getColors(count) {
    const availableColors = [
        '#36a2eb',
        '#ff6384',
        '#cc65fe',
        '#ffce56',
        '#30c589',
    ];

    // https://stackoverflow.com/questions/11935175/sampling-a-random-subset-from-an-array
    var shuffled = availableColors.slice(0), i = availableColors.length, temp, index;
    while (i--) {
        index = Math.floor((i + 1) * Math.random());
        temp = shuffled[index];
        shuffled[index] = shuffled[i];
        shuffled[i] = temp;
    }
    return shuffled.slice(0, count);
}

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
            chartConfig.data.datasets[0].backgroundColor = getColors(this.values.length)

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
        ],
        platform: null,
        versions: [
            {text: "All Versions", value: null},
        ],
        sgVersion: null,
        logText: "",
        hasError: false,
        errors: [],
        charts: {}
    },
    created: function () {
        // Fetch versions from server
        this.$http.get('/versions').then( response => {
            response.body.forEach(function(version) {
                this.versions.push(version);
            }, this);
        });

        // Same for platform types
        this.$http.get('/platforms').then( response => {
            response.body.forEach(function(platform) {
                this.platforms.push(platform);
            }, this);
        });
    },
    methods: {
        checkForm() {
            this.errors = [];

            if (!this.logText) {
                this.errors.push("Log text is required");
                this.errors.push("Another error!");
            }

            if (!this.errors.length) {
                this.hasError = false;
                return true;
            }
            else {
                this.hasError = true;
            }
        },
        getOccurrences(event) {
            this.checkForm();

            if (this.errors.length) {
                return;
            }

            this.$http.post('/occurrences', {logText: this.logText}).then( response => {
                this.charts = response.body;
            }, response => {
                alert("Error getting occurrences: " + response.status + "\n" + response.json());
            });
        }
    },
    delimiters: ['[[',']]']
})

