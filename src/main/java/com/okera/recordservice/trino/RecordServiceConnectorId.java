// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.okera.recordservice.trino;

import com.google.common.base.Objects;
import com.okera.recordservice.util.Preconditions;

public final class RecordServiceConnectorId {
  private final String id;

  public RecordServiceConnectorId(String id) {
    this.id = Preconditions.checkNotNull(id, "id is null");
  }

  @Override
  public String toString() {
    return id;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if ((obj == null) || (getClass() != obj.getClass())) return false;
    RecordServiceConnectorId other = (RecordServiceConnectorId) obj;
    return Objects.equal(this.id, other.id);
  }
}
