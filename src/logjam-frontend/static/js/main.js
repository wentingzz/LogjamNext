// vue-resources for http requests
Vue.use(VueResource);

function getColors(count) {
    /**
     * Returns a list of <count> unique colors in hex form
     */
    const availableColors = [
        '#36a2eb',
        '#ff6384',
        '#cc65fe',
        '#ffce56',
        '#30c589',
        '#0053b5',
    ];

    return availableColors.slice(0, count);
}

Vue.component('pie-chart', {
    /**
     * Reusable component for a chartjs pie chart
     */
    props: {
        title: String, // Title that is displayed above chart
        labels: Array, // List of the 'axis' labels in order
        values: Array  // List of data values in order matching labels
    },
    data: function() {
        return {
            // Config template for a pie chart. Data and labels will be filled in upon instantiation
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
            // Set title, labels, and data
            chartConfig.options.title.text = this.title;
            chartConfig.data.labels = this.labels;
            chartConfig.data.datasets[0].data = this.values;

            // Randomly pick some pretty colors
            chartConfig.data.datasets[0].backgroundColor = getColors(this.values.length)

            // Bind the chart component to the canvas (which is this object's $el property)
            const myChart = new Chart(this.$el, chartConfig);
      }
    },

    mounted() {
        // Runs when this object shows up on the page
        this.createChart(this.config);
    }
})


var vm = new Vue({
    el: "#app",
    data: {
        platforms: [
            "All Platforms"
        ],
        platform: "All Platforms",
        versions: [
            "All Versions"
	],
        sgVersion: "All Versions",
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
            /**
             * Validate form elements. Store any errors in this.errors and return false if any
             */
            this.errors = [];

            if (!this.logText) {
                this.errors.push("Log text is required");
            }

            if (!this.errors.length) {
                this.hasError = false;
                return true;
            }
            else {
                this.hasError = true;
                return false;
            }
        },
        getOccurrences(event) {
            /**
             * Make an API call to query for the given log text. Server should return data on what charts to display.
             */

            if (!this.checkForm()) {
                return;
            }

            this.charts = [];
            this.$http.post('/matchData', {logText: this.logText, sgVersion: this.sgVersion, platform: this.platform}).then( response => {
                this.charts = response.body;
            }, response => {
                alert("Error getting occurrences: " + response.status + "\n" + response.json());
            });
        }
    },
    delimiters: ['[[',']]'] // Change from the default {{ }} to work with Flask's Jinja templates
})

