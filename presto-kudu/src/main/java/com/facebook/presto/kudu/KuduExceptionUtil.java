/*
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
package com.facebook.presto.kudu;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;

public class KuduExceptionUtil
{
    private KuduExceptionUtil()
    {}

    public static PrestoException error(Throwable t)
    {
        throw new PrestoException(StandardErrorCode.GENERIC_EXTERNAL, t.getMessage(), t);
    }

    public static PrestoException error(String message, Throwable t)
    {
        throw new PrestoException(StandardErrorCode.GENERIC_EXTERNAL, message, t);
    }

    public static PrestoException error(String message)
    {
        throw new PrestoException(StandardErrorCode.GENERIC_EXTERNAL, message);
    }
}
