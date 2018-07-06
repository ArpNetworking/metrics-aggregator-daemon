/*
 * Copyright 2017 Dropbox, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.inscopemetrics.tsdcore.statistics;

import com.arpnetworking.logback.annotations.Loggable;

/**
 * Top 75th percentile statistic. Use <code>StatisticFactory</code> for construction.
 *
 * @author Gilligan Markham (gmarkham at dropbox dot com)
 */
@Loggable
public final class TP75Statistic extends TPStatistic {

    private TP75Statistic() {
        super(75d);
    }

    private static final long serialVersionUID = 1;
}
