/*
* Pedis is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* You may obtain a copy of the License at
*
*     http://www.gnu.org/licenses
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
* 
*  Copyright (c) 2016-2026, Peng Jian, pengjian.uestc@gmail.com. All rights reserved.
*
*/

#pragma once

enum class data_type : unsigned {
    deleted = 1,
    uint64  = 2,
    int64   = 3,
    numeric = 4,
    bytes   = 5,
    list    = 6,
    dict    = 7,
    set     = 8,
    sset    = 9,
    bitmap  = 10,
    hll     = 11,
};

