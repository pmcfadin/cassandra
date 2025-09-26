/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Simple charting wrapper for Cassandra Web Interface using uPlot
 * Provides time-series charts with axes, tooltips, and hover
 */
class ChartManager {
    constructor() {
        this.charts = new Map();
        this.isUPlotLoaded = false;
    }

    async ensureLibrariesLoaded() {
        if (this.isUPlotLoaded && window.uPlot) {
            return true;
        }

        try {
            await this.loadCSS('/assets/css/uPlot.min.css');
            await this.loadScript('/assets/js/uPlot.iife.min.js');
            this.isUPlotLoaded = true;
            return true;
        } catch (error) {
            console.error('Failed to load uPlot libraries:', error);
            return false;
        }
    }

    loadScript(src) {
        return new Promise((resolve, reject) => {
            if (document.querySelector(`script[src="${src}"]`)) {
                resolve();
                return;
            }

            const script = document.createElement('script');
            script.src = src;
            script.onload = resolve;
            script.onerror = reject;
            document.head.appendChild(script);
        });
    }

    loadCSS(href) {
        return new Promise((resolve) => {
            if (document.querySelector(`link[href="${href}"]`)) {
                resolve();
                return;
            }

            const link = document.createElement('link');
            link.rel = 'stylesheet';
            link.href = href;
            link.onload = resolve;
            link.onerror = resolve; // Don't fail on CSS errors
            document.head.appendChild(link);
        });
    }

    async createChart(elementId, options = {}) {
        if (!await this.ensureLibrariesLoaded()) {
            console.error('uPlot not available, chart creation failed');
            return null;
        }

        const {
            label = 'Metric',
            unit = '',
            color = '#007bff',
            maxPoints = 90
        } = options;

        const element = document.getElementById(elementId);
        if (!element) {
            console.error(`Element with id "${elementId}" not found`);
            return null;
        }


        // Clear the loading text before creating chart
        element.innerHTML = '';

        // Initialize data buffers
        const data = [[], []]; // [timestamps, values]

        // Calculate responsive dimensions based on uPlot documentation
        // uPlot's width/height includes the entire plot area INCLUDING axes

        // Force layout recalculation to get accurate dimensions
        element.style.display = 'flex';

        // Use the chart card as the reference instead of the chart container
        // since the container might be reporting wrong dimensions
        const parentChartCard = element.closest('.chart-card');
        let containerWidth, containerHeight;

        if (parentChartCard) {
            // Use chart card dimensions minus padding and title space
            const cardWidth = parentChartCard.clientWidth;
            const cardHeight = parentChartCard.clientHeight;
            // Account for card padding (20-28px) and title space (~60px)
            containerWidth = Math.max(300, cardWidth - 50);
            containerHeight = Math.max(200, cardHeight - 120);
        } else {
            // Fallback to element dimensions with safety bounds
            const rawWidth = element.offsetWidth || element.clientWidth || 400;
            const rawHeight = element.offsetHeight || element.clientHeight || 300;
            // Cap at reasonable maximums to prevent massive charts
            containerWidth = Math.min(1200, Math.max(300, rawWidth));
            containerHeight = Math.min(800, Math.max(200, rawHeight));
        }

        // Give most of the container space to the chart (accounting for container padding/border)
        const chartWidth = Math.max(380, containerWidth - 12);
        const chartHeight = Math.max(240, containerHeight - 12);


        // uPlot configuration
        const opts = {
            width: chartWidth,
            height: chartHeight,
            series: [
                {
                    label: 'Time'
                },
                {
                    label: label,
                    stroke: color,
                    width: 2,
                    fill: color + '20'
                }
            ],
            scales: {
                x: {
                    time: true,
                    range: (self, initMin, initMax) => {
                        // Show last 15 minutes
                        const now = Date.now() / 1000;
                        return [now - 900, now];
                    }
                },
                y: {
                    auto: true,
                    range: (self, initMin, initMax) => {
                        if (initMin === initMax) {
                            return [initMin - 1, initMax + 1];
                        }
                        const padding = (initMax - initMin) * 0.1;
                        return [Math.max(0, initMin - padding), initMax + padding];
                    }
                }
            },
            axes: [
                {
                    // X-axis (time)
                    grid: { show: true },
                    ticks: { show: true },
                    font: '11px sans-serif',
                    size: 30  // Height of x-axis area
                },
                {
                    // Y-axis (values)
                    grid: { show: true },
                    ticks: { show: true },
                    font: '11px sans-serif',
                    size: 45,  // Width of y-axis area
                    values: (self, ticks) => ticks.map(v => v.toFixed(2) + unit)
                }
            ],
            cursor: {
                show: true,
                sync: {
                    key: 'cassandra-metrics'
                }
            },
            legend: {
                show: true,
                live: true
            },
            hooks: {
                setCursor: [
                    (self) => {
                        const { left, top, idx } = self.cursor;

                        if (idx !== null && left >= 0 && top >= 0) {
                            const timestamp = data[0][idx];
                            const value = data[1][idx];

                            if (timestamp !== undefined && value !== null && value !== undefined) {
                                const time = new Date(timestamp * 1000);
                                const timeStr = time.toLocaleTimeString('en-US', { hour12: false });
                                const valueStr = value.toFixed(2) + unit;

                                self.over.title = `${timeStr} — ${valueStr}`;
                            }
                        }
                    }
                ]
            }
        };

        const chart = new window.uPlot(opts, data, element);

        // Ensure proper canvas sizing - fix common uPlot dimension issues
        setTimeout(() => {
            chart.setSize({ width: chartWidth, height: chartHeight });

            // Force correct canvas dimensions
            const canvasElements = element.querySelectorAll('canvas');
            canvasElements.forEach(canvas => {
                canvas.width = chartWidth;
                canvas.height = chartHeight;
                canvas.style.width = chartWidth + 'px';
                canvas.style.height = chartHeight + 'px';
                canvas.style.display = 'block';
            });

            // Ensure wrapper has correct size
            const uplotWrapper = element.querySelector('.u-wrap');
            if (uplotWrapper) {
                uplotWrapper.style.width = chartWidth + 'px';
                uplotWrapper.style.height = chartHeight + 'px';
                uplotWrapper.style.display = 'block';
            }

            chart.setData(data);
        }, 100);

        const chartHandle = {
            data,
            chart,
            maxPoints,
            element,
            update: (timestamp, value) => {
                // Add new point
                data[0].push(timestamp);
                data[1].push(value);

                // Trim to max points
                if (data[0].length > maxPoints) {
                    data[0].shift();
                    data[1].shift();
                }

                // Update chart
                chart.setData(data);
            },
            resize: () => {
                // Use same logic as initial creation
                const resizeChartCard = element.closest('.chart-card');
                let containerWidth, containerHeight;

                if (resizeChartCard) {
                    const cardWidth = resizeChartCard.clientWidth;
                    const cardHeight = resizeChartCard.clientHeight;
                    containerWidth = Math.max(300, cardWidth - 50);
                    containerHeight = Math.max(200, cardHeight - 120);
                } else {
                    const rawWidth = element.offsetWidth || element.clientWidth || 400;
                    const rawHeight = element.offsetHeight || element.clientHeight || 300;
                    containerWidth = Math.min(1200, Math.max(300, rawWidth));
                    containerHeight = Math.min(800, Math.max(200, rawHeight));
                }

                const newWidth = Math.max(380, containerWidth - 12);
                const newHeight = Math.max(240, containerHeight - 12);

                chart.setSize({ width: newWidth, height: newHeight });
            },
            destroy: () => {
                chart.destroy();
            }
        };

        this.charts.set(elementId, chartHandle);
        return chartHandle;
    }

    getChart(elementId) {
        return this.charts.get(elementId);
    }

    destroyChart(elementId) {
        const chartHandle = this.charts.get(elementId);
        if (chartHandle) {
            chartHandle.destroy();
            this.charts.delete(elementId);
        }
    }

    resizeAll() {
        for (const [id, handle] of this.charts) {
            if (handle.resize) {
                handle.resize();
            }
        }
    }

    destroyAll() {
        for (const [id, handle] of this.charts) {
            handle.destroy();
        }
        this.charts.clear();
    }
}

// Global chart manager instance
window.chartManager = new ChartManager();