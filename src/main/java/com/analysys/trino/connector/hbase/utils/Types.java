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
package com.analysys.trino.connector.hbase.utils;

import io.airlift.slice.Slice;
import io.trino.spi.type.DecimalType;

import java.math.BigDecimal;
import java.math.BigInteger;

import static com.analysys.trino.connector.hbase.utils.UnscaledDecimal128Arithmetic.unscaledDecimal;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.lang.String.format;

/**
 * types
 *
 * @author wupeng
 * @date 2019/01/29
 */
public class Types {
    private static final int NUMBER_OF_LONGS = 2;
    public static final int UNSCALED_DECIMAL_128_SLICE_LENGTH = NUMBER_OF_LONGS * SIZE_OF_LONG;

    private Types() {
    }

    public static <A, B extends A> B checkType(A value, Class<B> target, String name) {
        if (value == null) {
            throw new NullPointerException(format("%s is null", name));
        }
        checkArgument(target.isInstance(value),
                "%s must be of type %s, not %s",
                name,
                target.getName(),
                value.getClass().getName());
        return target.cast(value);
    }

    /**
     * Converts {@link BigDecimal} to {@link Slice} representing it for long {@link DecimalType}.
     * It is caller responsibility to ensure that {@code value.scale()} equals to {@link DecimalType#getScale()}.
     */
    public static Slice encodeScaledValue(BigDecimal value) {
        return encodeUnscaledValue(value.unscaledValue());
    }

    public static Slice encodeUnscaledValue(BigInteger unscaledValue) {
        return unscaledDecimal(unscaledValue);
    }


}
