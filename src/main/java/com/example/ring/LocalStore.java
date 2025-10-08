package com.example.ring;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class LocalStore {
    private List<DataItem> localStore;

    LocalStore() {
        localStore = new ArrayList<DataItem>();
    }

    public DataItem get(Long key) {
        return localStore.stream().filter(p -> p.getKey().equals(key)).findFirst().orElse(null);
    }

    public void store(DataItem dataItem) {
        DataItem temp = this.get(dataItem.getKey());
        if  (temp == null) {
            localStore.add(dataItem);
        }  else {
            update(dataItem);
        }
    }

    public void update(DataItem dataItem) {
        DataItem temp = this.get(dataItem.getKey());
        if (temp == null) {
            store(dataItem);
        }  else {
            int v = temp.getVersion();
            temp.setVersion(v + 1);
            temp.setValue(dataItem.getValue());
        }
    }

    public Set<Long> getKeys() {
        return localStore.stream()
            .map(DataItem::getKey)   // extract the key (id)
            .collect(Collectors.toSet());
    }

    public void remove(Long key) {
        DataItem temp = this.get(key);
        if (temp != null) {
            localStore.remove(temp);
        }
    }
}
