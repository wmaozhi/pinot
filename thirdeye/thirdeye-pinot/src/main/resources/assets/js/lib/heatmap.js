function getHeatmap(tab) {

    //Todo: add the real endpoint
    var url = "/dashboard/data/heatmap?" + window.location.hash.substring(1);
    getData(url, tab).done(function (heatMapData) {


        //AJAX
        var url = "/dashboard/summary/autoDimensionOrder?" +
            "dataset="   + hash.dataset + //"thirdeyeKbmi" +
            "&metrics=" + hash.metrics + //"desktopPageViews" +
            "&baselineStart="+ hash.baselineStart + //"1470553200000" +
            "&currentStart="+ hash.currentStart + //"1471158000000" +
            "&aggTimeGranularity=" + "DAYS" +// hash.aggTimeGranularity +
            "&dimensions="+ hash.dimensions + // "browserName,continent,countryCode,deviceName,environment,locale,osName,pageKey,service,sourceApp" +
            "&topDimensions=3" +
            "&oneSideError=true" +
            "&summarySize=15"

        getData(url).done(function(summaryData){

            console.log("summaryData")
            console.log(summaryData)


            renderD3heatmap(heatMapData, summaryData, tab);

            heatMapEventListeners(tab);

        })

    });
};

function renderD3heatmap(heatMapData, summaryData, tab) {
    var data = heatMapData;
    //Error handling when data is falsy (empty, undefined or null)
    if (!data) {
        $("#" + tab + "-chart-area-error").empty()
        var warning = $('<div></div>', { class: 'uk-alert uk-alert-warning' })
        warning.append($('<p></p>', { html: 'Something went wrong. Please try and reload the page. Error: data =' + data  }))
        $("#" + tab + "-chart-area-error").append(warning)
        $("#" + tab + "-chart-area-error").show()
        return
    } else {
        $("#" + tab + "-chart-area-error").hide()
    }

    //var summaryData = {"dimensions":["continent","countryCode","pageKey"],"responseRows":[{"names":["(ALL)-","(ALL)","(ALL)"],"baselineValue":290076771,"currentValue":290989633,"ratio":1.0031469669110458},{"names":["unknown","(ALL)-","(ALL)"],"baselineValue":49637728,"currentValue":42080059,"ratio":0.8477434543337681},{"names":["unknown","other","p_flagship3_feed_updates"],"baselineValue":12438174,"currentValue":8903781,"ratio":0.7158430972263292},{"names":["Oceania","au","(ALL)"],"baselineValue":17854104,"currentValue":15000436,"ratio":0.8401673923261564},{"names":["North America","us","(ALL)-"],"baselineValue":120970350,"currentValue":105194842,"ratio":0.8695919454643225},{"names":["North America","us","p_flagship3_people_pymk"],"baselineValue":11642301,"currentValue":8071474,"ratio":0.693288551807757},{"names":["North America","us","p_flagship3_feed_updates"],"baselineValue":24534044,"currentValue":17338875,"ratio":0.7067271502407023},{"names":["North America","ca","(ALL)"],"baselineValue":21769165,"currentValue":18074492,"ratio":0.8302795261095224},{"names":["Latin America","br","(ALL)"],"baselineValue":22534261,"currentValue":17793288,"ratio":0.7896104513922156},{"names":["Europe","fr","(ALL)"],"baselineValue":20051472,"currentValue":16703293,"ratio":0.8330207877007733}]};

    /* Handelbars template for treemap table */
    var combinedData = {heatMapData : heatMapData, summaryData : summaryData}
    var result_treemap_template = HandleBarsTemplates.template_treemap(combinedData)
    $("#" + tab + "-display-chart-section").html(result_treemap_template);

    //var invertColorMetrics
    var baseForLtZero = 'rgba(255,0,0,'; //lt zero is default red
    var baseForGtZero = 'rgba(0,0,255,'; //gt zero is default blue
    var invertColorMetrics = window.datasetConfig.invertColorMetrics;

    var numMetrics = data["metrics"].length
    for (var m = 0; m < numMetrics; m++) {
        var metric = data["metrics"][m];

        var numDimensions = data["dimensions"].length
        for (var d = 0; d < numDimensions; d++) {
            var dimension = data["dimensions"][d];


            var dimensionData = data["data"][metric + "." + dimension]["responseData"]
            var schema = data["data"][metric + "." + dimension]["schema"]["columnsToIndexMapping"]


            //PARSE DATA
            var root_0 = {}
            var root_1 = {}
            var root_2 = {}
            root_0.name = dimension;
            root_1.name = dimension;
            root_2.name = dimension;

            root_0.metric = metric;
            root_1.metric = metric;
            root_2.metric = metric;
            if (typeof invertColorMetrics !== "undefined" && invertColorMetrics.indexOf(metric) > -1) { // invert
                baseForLtZero = 'rgba(0,0,255,'; //lt zero becomes blue
                baseForGtZero = 'rgba(255,0,0,'; //gt zero becomes red
            }


            var children_0 = [];
            var children_1 = [];
            var children_2 = [];
            var numDimValues = dimensionData.length;
            for (valId = 0; valId < numDimValues; valId++) {

                var dimensionValue = dimensionData[valId][schema["dimensionValue"]]
                //Todo: remove this "" handler once backend is adding it to other
                if (dimensionValue == "") {
                    dimensionValue = "UNKNOWN";
                }
                ;

                var color_0 = parseFloat(dimensionData[valId][schema["deltaColor"]]); //percentageChange
                var color_1 = parseFloat(dimensionData[valId][schema["contributionColor"]]); //baselineContribution
                var color_2 = parseFloat(dimensionData[valId][schema["contributionToOverallColor"]]); // contributionToOverallChange

                var delta_0 = parseFloat(dimensionData[valId][schema["percentageChange"]]);
                var delta_1 = parseFloat(dimensionData[valId][schema["contributionDifference"]]);
                var delta_2 = parseFloat(dimensionData[valId][schema["contributionToOverallChange"]]);

                var opacity_0 = parseFloat(Math.abs(Math.round(color_0)) / 25);
                var opacity_1 = parseFloat(Math.abs(Math.round(color_1)) / 25);
                var opacity_2 = parseFloat(Math.abs(Math.round(color_2)) / 25);

                var fontColor_0 = opacity_0 < 0.3 ? '#000000' : '#ffffff';
                var fontColor_1 = opacity_1 < 0.3 ? '#000000' : '#ffffff';
                var fontColor_2 = opacity_2 < 0.3 ? '#000000' : '#ffffff';

                var backgroundColor_0 = Math.round(color_0) < 0 ? baseForLtZero + opacity_0 + ")" : ( Math.round(color_0) > 0 ? baseForGtZero + opacity_0 + ")" : "rgba(221,221,221,1)");
                var backgroundColor_1 = Math.round(color_1) < 0 ? baseForLtZero + opacity_1 + ")" : ( Math.round(color_1) > 0 ? baseForGtZero + opacity_1 + ")" : "rgba(221,221,221,1)");
                var backgroundColor_2 = Math.round(color_2) < 0 ? baseForLtZero + opacity_2 + ")" : ( Math.round(color_2) > 0 ? baseForGtZero + opacity_2 + ")" : "rgba(221,221,221,1)");

                var label_0 = delta_0 <= 0 ? dimensionValue + " (" + delta_0 + "%)" : dimensionValue + " (+" + delta_0 + "%)";
                var label_1 = delta_1 <= 0 ? dimensionValue + " (" + delta_1 + "%)" : dimensionValue + " (+" + delta_1 + "%)";
                var label_2 = delta_2 <= 0 ? dimensionValue + " (" + delta_2 + "%)" : dimensionValue + " (+" + delta_2 + "%)";


                var size = parseFloat(dimensionData[valId][schema["deltaSize"]]);

                children_0.push({ "name": dimensionValue, "size": size, "label": label_0, "bgcolor": backgroundColor_0, "color": fontColor_0, "valueId": valId, "tooltip": tooltip});
                children_1.push({ "name": dimensionValue, "size": size, "label": label_1, "bgcolor": backgroundColor_1, "color": fontColor_1, "valueId": valId, "tooltip": tooltip});
                children_2.push({ "name": dimensionValue, "size": size, "label": label_2, "bgcolor": backgroundColor_2, "color": fontColor_2, "valueId": valId, "tooltip": tooltip});

            }
            root_0.children = children_0;
            root_1.children = children_1;
            root_2.children = children_2;

            //CHART LAYOUT
            var margin = {top: 0, right: 0, bottom: 5, left: 0};
            var width = 0.65 * window.innerWidth;
            var height = window.innerHeight * 0.7 / numDimensions;

            var placeholder_0 = '#metric_' + metric + '_dim_' + d + '_treemap_0'
            var placeholder_1 = '#metric_' + metric + '_dim_' + d + '_treemap_1'
            var placeholder_2 = '#metric_' + metric + '_dim_' + d + '_treemap_2'

            var mousemove = function (d) {

                var tooltipWidthPx = $("#tooltip").css("width");
                var tooltipHeightPx = $("#tooltip").css("height");
                var tooltipWidth = tooltipWidthPx.substring(0, tooltipWidthPx.length - 2);


                var target = $(this);
                var dimension = target.attr("data-dimension");
                var metric = target.attr("data-metric");
                var valueId = target.attr("data-value-id");
                var value = target.attr("id");

                var treemapOffset = $($(".dimension-treemap")[0]).offset();

                var tooltipHeightOffset = parseInt(tooltipHeightPx.substring(0, tooltipHeightPx.length - 2)) / 2;
                var tooltipWidthOffset = tooltipWidth / 2;

                var directionX = d3.event.pageX - treemapOffset.left < (window.innerWidth - treemapOffset.left) / 2 ? "-1" : "1";
                var directionY = d3.event.pageY - treemapOffset.top < window.innerHeight / 2 ? "-1" : "1";
                var distanceFromMousePointer = 50;

                var xPosition = d3.event.pageX - treemapOffset.left - directionX * (tooltipWidthOffset + distanceFromMousePointer);
                var yPosition = d3.event.pageY - (tooltipHeightOffset + distanceFromMousePointer);

                var dimData = data["data"][metric + "." + dimension]["responseData"]

                var cellSizeExpression = dimData[valueId][schema["cellSizeExpression"]];

                d3.select("#tooltip")
                    .style("left", xPosition + "px")
                    .style("top", yPosition + "px");

                d3.select("#tooltip #dim-value")
                    .text(value);

                d3.select("#tooltip #delta").text(dimData[valueId][schema["percentageChange"]] + '%');
                d3.select("#tooltip #baseline-contribution").text(dimData[valueId][schema["baselineContribution"]]);
                d3.select("#tooltip #current-contribution").text(dimData[valueId][schema["currentContribution"]]);
                d3.select("#tooltip #contribution-diff").text(dimData[valueId][schema["contributionDifference"]] + '%');

                if (cellSizeExpression != null && undefined != cellSizeExpression) {
                    var cellSizeExpressionDisplay = dimData[valueId][schema["deltaSize"]] + " (" + cellSizeExpression + ")";
                    d3.select("#tooltip #baseline-value").text(dimData[valueId][schema["baselineValue"]] + " (" + dimData[valueId][schema["numeratorBaseline"]] + "/" + dimData[valueId][schema["denominatorBaseline"]] + ")");
                    d3.select("#tooltip #current-value").text(dimData[valueId][schema["currentValue"]] + " (" + dimData[valueId][schema["numeratorCurrent"]] + "/" + dimData[valueId][schema["denominatorCurrent"]] + ")");
                    d3.select("#tooltip #cell-size").text(cellSizeExpressionDisplay);
                } else {
                    d3.select("#tooltip #baseline-value").text(dimData[valueId][schema["baselineValue"]]);
                    d3.select("#tooltip #current-value").text(dimData[valueId][schema["currentValue"]]);
                }
                d3.select("#tooltip").classed("hidden", false);
            };

            var mouseleave = function () {
                d3.select("#tooltip").classed("hidden", true);
            };

            //DRAW TREEMAP
            function drawTreemap(root, placeholder) {
                var treemap = d3.layout.treemap()
                    .size([width, height])
                    .sticky(true)
                    .sort(function (a, b) {
                        return a.value - b.value;
                    })
                    .value(function (d) {
                        return d.size;
                    });

                var div = d3.select(placeholder).append("div")
                    .style("position", "relative")
                    .style("width", width + "px")
                    .style("height", (height + margin.top + margin.bottom) + "px")
                    .style("left", margin.left + "px")
                    .style("top", margin.top + "px");

                var node = div.datum(root).selectAll(".node")
                    .data(treemap.nodes)
                    .enter().append("div")
                    .attr("class", "node")
                    .attr("data-dimension", function (d) {
                        return root.name
                    })
                    .attr("data-metric", function (d) {
                        return root.metric
                    })
                    .attr("data-value-id", function (d) {
                        return d.valueId
                    })
                    .attr("id", function (d) {
                        return d.name
                    })
                    .on("mousemove", mousemove)
                    .on("mouseout", mouseleave)
                    .call(position)
                    .style("background", function (d) {
                        return d.children ? null : d.bgcolor
                    })
                    .style("text-align", "center")
                    .style("color", function (d) {
                        return d.color
                    })
                    .style("font-size", "12px")
                    .style("display", "flex")
                    .style("align-items", "center")
                    .style("justify-content", "center")
                    .text(function (d) {
                        return d.children ? null : d.label
                    });

                function position() {
                    this.style("left", function (d) {
                        return d.x + "px";
                    })
                        .style("top", function (d) {
                            return d.y + "px";
                        })
                        .style("width", function (d) {
                            return Math.max(0, d.dx - 1) + "px";
                        })
                        .style("height", function (d) {
                            return Math.max(0, d.dy - 1) + "px";
                        });
                }
            }

            drawTreemap(root_0, placeholder_0)
            drawTreemap(root_1, placeholder_1)
            drawTreemap(root_2, placeholder_2)

        }


        //Create dataTable instance of summary table
        $(".difference-summary").each(function(){
            var metricName = $(this).attr("data-metric");
            console.log("#heat-map-" + metric +"-difference-summary-table")
            $("#heat-map-" + metric +"-difference-summary-table").DataTable();
        })

    }
}

function heatMapEventListeners(tab) {
    //Treemap eventlisteners

    $(".dimension-treemap-mode").click(function () {

        var currentMode = $(this).attr('mode');
        var currentMetricArea = $(this).closest(".dimension-heat-map-treemap-section");

        // Display related treemap
        $(".treemap-container", currentMetricArea).hide();
        $($(".treemap-container", currentMetricArea)[currentMode]).show();

        //Change icon on the radio buttons
        $('.dimension-treemap-mode i', currentMetricArea).removeClass("uk-icon-eye");
        $('.dimension-treemap-mode i', currentMetricArea).addClass("uk-icon-eye-slash");
        $('i', this).removeClass("uk-icon-eye-slash");
        $('i', this).addClass("uk-icon-eye");
    });

    //Set initial view

    //Preselect treeemap mode on pageload (mode 0 = Percentage Change)
    $(".dimension-treemap-mode[mode = '0']").click()

    //Indicate baseline total value increase/decrease with red/blue colors next to the title of the table and the treemap
    $(".title-box .delta-ratio, .title-box .delta-value").each(function (index, currentDelta) {

        var delta = $(currentDelta).html().trim().replace(/[\$,]/g, '') * 1

        if (delta != 0 && !isNaN(delta)) {
            var color = delta > 0 ? "blue plus-symbol" : "red"

            $(currentDelta).addClass(color)
        }
    })

    //Clicking a hetamap cell should fix the value in the filter
    $("#" + tab + "-display-chart-section").on("click", "div.node", function () {

        var dimensionValue = $(this).attr("id");
        if (dimensionValue.toLowerCase() == "other" || dimensionValue.toLowerCase() == "unknown") {
            alert("'Other' or 'unknown' value cannot be the filter.");
        } else {

            var dimension = $(this).attr("data-dimension")
            var filters = readFiltersAppliedInCurrentView(hash.view);
            filters[dimension] = [dimensionValue];

            updateFilterSelection(filters);

            hash.filters = encodeURIComponent(JSON.stringify(filters));
            hash.aggTimeGranularity = "aggregateAll";

            //update hash will trigger window.onhashchange event:
            //update the form area and trigger the ajax call
            window.location.hash = encodeHashParameters(hash);
        }
    })

}
