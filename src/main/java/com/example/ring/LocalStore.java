package com.example.ring;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A simple in-memory key-value store for a {@link NodeActor}.
 *
 * <p>This class provides a basic, non-thread-safe storage mechanism for {@link DataItem} objects.
 * It uses an {@link ArrayList} as the underlying data structure and offers methods for storing,
 * retrieving, updating, and removing data items based on their keys.
 *
 * <p>Its primary functions are:
 * <ul>
 *     <li>Storing {@link DataItem} objects, handling both new insertions and updates to existing items.</li>
 *     <li>Retrieving items by their unique key.</li>
 *     <li>Removing items from the store.</li>
 *     <li>Providing a set of all keys currently in the store, which is used for rebalancing purposes.</li>
 * </ul>
 * This class is designed to be used within the single-threaded execution context of an Akka actor.
 */
public class LocalStore {
    private List<DataItem> localStore;

    LocalStore() {
        localStore = new ArrayList<DataItem>();
    }

    /**
     * Retrieves a {@link DataItem} from the store by its key.
     *
     * @param key The key of the data item to retrieve.
     * @return The {@link DataItem} if found, otherwise {@code null}.
     */
    public DataItem get(Long key) {
        return localStore.stream().filter(p -> p.getKey().equals(key)).findFirst().orElse(null);
    }

    /**
     * Stores or updates a {@link DataItem} in the store.
     * <p>If an item with the same key already exists, it will be updated. Otherwise, the new item is added.
     *
     * @param dataItem The {@link DataItem} to be stored.
     */
    public void store(DataItem dataItem) {
        DataItem temp = this.get(dataItem.getKey());
        if  (temp == null) {
            localStore.add(dataItem);
        }  else {
            update(dataItem);
        }
    }

    /**
     * Updates an existing {@link DataItem} in the store.
     * <p>If the item exists, its version is incremented, and its value is updated. If the item does not exist,
     * it is added to the store as a new item.
     *
     * @param dataItem The {@link DataItem} containing the new value and the key of the item to update.
     */
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

    /**
     * Returns a set of all keys currently present in the store.
     *
     * @return A {@link Set} of all data item keys.
     */
    public Set<Long> getKeys() {
        return localStore.stream()
            .map(DataItem::getKey)   // extract the key (id)
            .collect(Collectors.toSet());
    }

    /**
     * Removes a {@link DataItem} from the store by its key.
     *
     * @param key The key of the data item to remove.
     */
    public void remove(Long key) {
        DataItem temp = this.get(key);
        if (temp != null) {
            localStore.remove(temp);
        }
    }
}
