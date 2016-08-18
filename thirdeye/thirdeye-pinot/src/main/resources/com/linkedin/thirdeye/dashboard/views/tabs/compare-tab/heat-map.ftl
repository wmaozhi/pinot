<section id="dimension-heat-map-section">
    <script id="treemap-template" type="text/x-handlebars-template">
        {{#with heatMapData}}
        {{#each metrics as |metricName metricIndex|}}
        <div class="metric-section-wrapper" rel="{{metricName}}">
            <div class="dimension-heat-map-treemap-section">
                <div class="title-box">
                    <table>
                        <tbody>
                        <tr>
                            <th>BASELINE</th>
                            <th><b>Start:</b></th>
                            <td class="baseline-date-time">{{millisToDate  @root/heatMapData/summary/simpleFields/baselineStart}}</td>
                            <th><b>End:</b></th>
                            <td class="baseline-date-time">{{millisToDate  @root/heatMapData/summary/simpleFields/baselineEnd}}</td>
                        </tr>
                        <tr>
                            <th>CURRENT</th>
                            <th><b>Start:</b></th>
                            <td class="current-date-time">{{millisToDate  @root/heatMapData/summary/simpleFields/currentStart}}</td>
                            <th><b>End:</b></th>
                            <td class="current-date-time">{{millisToDate  @root/heatMapData/summary/simpleFields/currentEnd}}</td>
                        </tr>
                        <tr><td colspan="5" style="border-bottom: 1px solid #ddd;"></td></tr>


                        <tr>
                            <th>Metric:</th>
                            <th> Baseline Total:</th>
                            <th> Current Total:</th>
                            <th> Delta Value:</th>
                            <th> Delta (%):</th>
                        </tr>
                        <tr>
                            <td class="title-stat">{{metricName}}  {{lookupInMapByKey @root/heatMapData/metricExpression  metricName}} </td>
                            <td class="title-stat baseline-total">{{@root/heatMapData/summary/simpleFields/baselineTotal}}</td>
                            <td class="title-stat current-total">{{@root/heatMapData/summary/simpleFields/currentTotal}}</td>
                            <td class="title-stat delta-value">{{@root/heatMapData/summary/simpleFields/deltaChange}} </td>
                            <td class="title-stat delta-ratio">{{@root/heatMapData/summary/simpleFields/deltaPercentage}}</td>
                        </tr>
                        </tbody>
                    </table>
                </div>

                <div class="uk-button-group dimension-treemap-toggle-buttons" data-uk-button-radio>
                    <button id="treemap_contribution-total-change-percent" class="uk-button dimension-treemap-mode" mode="0">
                        <i class="uk-icon-eye-slash"></i> Percentage Change
                    </button>
                    <button id="treemap_contribution-total-percent" class="uk-button dimension-treemap-mode" mode="1">
                        <i class="uk-icon-eye-slash"></i> Contribution Change (%)
                    </button>
                    <button id="treemap_contribution-change-percent" class="uk-button dimension-treemap-mode" mode="2">
                        <i class="uk-icon-eye-slash"></i> Contribution to overall Change (%)
                    </button>
                    <button id="treemap_contribution-difference-summary" class="uk-button dimension-treemap-mode" mode="3">
                        <i class="uk-icon-eye-slash"></i> Difference Summary
                    </button>
                </div>

                <div id="metric_{{metricName}}_treemap_0" class="treemap-container  uk-margin" mode="0">
                    <table class="treemap-display-tbl" style="position: relative; width: 100%;">
                        {{#each @root/heatMapData/dimensions as |dimensionName dimensionIndex|}}
                        <tr style="position: relative; width: 100%;">
                            <td class="treemap-display-tbl-dim"><div style="text-align: left;">{{dimensionName}}</div></td><td id="metric_{{metricName}}_dim_{{dimensionIndex}}_treemap_0" class="dimension-treemap" rel="{{dimensionName}}" style="position: relative; left: 0px; top: 0px; width: 100%;"></td>
                        </tr>
                        {{/each}}
                    </table>
                </div>

                <div id="metric_{{metricName}}_treemap_1" class="treemap-container  uk-margin" mode="1">
                    <table class="treemap-display-tbl" style="position: relative; width: 100%;">
                        {{#each @root/heatMapData/dimensions as |dimensionName dimensionIndex|}}
                        <tr style="position: relative; width: 100%;">
                            <td class="treemap-display-tbl-dim"><div style="text-align: left;">{{dimensionName}}</div></td><td id="metric_{{metricName}}_dim_{{dimensionIndex}}_treemap_1" class="dimension-treemap" rel="{{dimensionName}}" style="position: relative; left: 0px; top: 0px; width: 100%;" ></td>
                        </tr>
                        {{/each}}
                    </table>
                </div>

                <div id="metric_{{metricName}}_treemap_2" class="treemap-container  uk-margin" mode="2">
                    <table class="treemap-display-tbl" style="position: relative; width: 100%;">
                        {{#each @root/heatMapData/dimensions as |dimensionName dimensionIndex|}}
                        <tr style="position: relative; width: 100%;">
                            <td class="treemap-display-tbl-dim"><div style="text-align: left;">{{dimensionName}}</div></td><td id="metric_{{metricName}}_dim_{{dimensionIndex}}_treemap_2" class="dimension-treemap" rel="{{dimensionName}}" style="position: relative; left: 0px; top: 0px; width: 100%;" ></td>
                        </tr>
                        {{/each}}
                    </table>
                </div>
                <div id="difference-summary-{{metricName}}" class="difference-summary uk-margin-bottom" data-metric="{{metricName}}">
                    <h3>Summary</h3>
                    <table id="heat-map-{{metricName}}-difference-summary-table">

                        {{#with @root/summaryData}}
                        <thead>
                        <tr>
                            <th colspan="{{dimensions.length}}">Dimension</th>
                            <th></th>
                            <th></th>
                            <th></th>
                        </tr>
                        <tr>
                            {{#with @root/summaryData/dimensions}}
                            {{#each this as |dimensionName dimensionIndex|}}
                                <th>{{dimensionName}}</th>
                            {{/each}}
                            {{/with}}
                                <th>Baseline</th>
                                <th>Current</th>
                                <th>Ratio</th>
                        </tr>
                        </thead>
                        {{#with @root/summaryData/responseRows}}
                        <tbody>
                        {{#each this as |row rowIndex|}}
                        <tr>
                            {{#each row.names as |dimensionValue dimension|}}
                              <td style="background-color: rgba(222, 222, 222, 0.5);">{{dimensionValue}}</td>
                            {{/each}}
                            <td>{{row.baselineValue}}</td>
                            <td>{{row.currentValue}}</td>
                            <td>{{row.ratio}}</td>
                        </tr>
                        {{/each}}
                        {{/with}}<!--end of summaryData -->
                        </tbody>
                        {{/with}}<!--end of summaryData scope-->
                    </table>
                </div>
            </div>
        </div>
        {{/each}}
        {{/with}}
        <div id="tooltip" class="hidden">
            <table>
                <tr><td>value</td><td id="dim-value"></td></tr>
                <tr><td>baseline value</td><td id="baseline-value"></td></tr>
                <tr><td>current value</td><td id="current-value"></td></tr>
                <tr id="cell-size-row" class="hidden"><td>cell size</td><td id="cell-size"></td></tr>
                <tr><td>percentage change </td><td id="delta"></td></tr>
                <tr><td>baseline contribution</td><td id="baseline-contribution"></td></tr>
                <tr><td>current contribution</td><td id="current-contribution"></td></tr>
                <tr><td>contribution change   </td><td id="contribution-diff"></td></tr>
            </table>
        </div>
    </script>
</section>
