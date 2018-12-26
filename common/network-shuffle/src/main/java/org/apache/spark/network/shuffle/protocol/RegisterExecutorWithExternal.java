/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.network.shuffle.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

import org.apache.spark.network.protocol.Encoders;

// Needed by ScalaDoc. See SPARK-7726
import static org.apache.spark.network.shuffle.protocol.BlockTransferMessage.Type;

public class RegisterExecutorWithExternal extends BlockTransferMessage {

    public final String appId;
    public final String execId;
    public final String shuffleManager;

    public RegisterExecutorWithExternal(
            String appId, String execId, String shuffleManager) {
        this.appId = appId;
        this.execId = execId;
        this.shuffleManager = shuffleManager;
    }

    @Override
    protected Type type() {
        return Type.REGISTER_EXECUTOR_WITH_EXTERNAL;
    }

    @Override
    public int encodedLength() {
        return Encoders.Strings.encodedLength(appId)
                + Encoders.Strings.encodedLength(execId)
                + Encoders.Strings.encodedLength(shuffleManager);
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, appId);
        Encoders.Strings.encode(buf, execId);
        Encoders.Strings.encode(buf, shuffleManager);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof RegisterExecutorWithExternal) {
            RegisterExecutorWithExternal o = (RegisterExecutorWithExternal) other;
            return Objects.equal(appId, o.appId)
                    && Objects.equal(execId, o.execId)
                    && Objects.equal(shuffleManager, o.shuffleManager);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(appId, execId, shuffleManager);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(RegisterExecutorWithExternal.class)
                .add("appId", appId)
                .add("execId", execId)
                .add("shuffleManager", shuffleManager)
                .toString();
    }

    public static RegisterExecutorWithExternal decode(ByteBuf buf) {
        String appId = Encoders.Strings.decode(buf);
        String execId = Encoders.Strings.decode(buf);
        String shuffleManager = Encoders.Strings.decode(buf);
        return new RegisterExecutorWithExternal(appId, execId, shuffleManager);
    }
}
