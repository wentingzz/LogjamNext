Vue.component('pie-chart', {
    props: [],
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
                        ],
                        label: 'Dataset 1'
                    }],
                    labels: [
                        'Occurs',
                        'Does not occur',
                    ]
                },
                options: {
                    responsive: true
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

