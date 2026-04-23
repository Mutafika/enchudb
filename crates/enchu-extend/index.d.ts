/** Initialize Enchu v20 cache */
export function init(cacheDir: string, schemaJson: string, pgConn: string): void

/** Open existing cache */
export function open(cacheDir: string, schemaJson: string, pgConn: string): void

/** Insert root entity → returns entity_id */
export function insert(table: string): number

/** Insert child entity with parent link → returns entity_id */
export function insertChild(table: string, parentId: number): number

/** Write text (Symbol) field */
export function writeText(table: string, entityId: number, field: string, value: string): void

/** Write integer field */
export function writeInt(table: string, entityId: number, field: string, value: number): void

/** Cylinder slice by value_id → entity_ids (2ns + FFI) */
export function slice(table: string, field: string, valueId: number): number[]

/** Cylinder range slice → entity_ids (2ns + FFI) */
export function sliceRange(table: string, field: string, low: number, high: number): number[]

/** Read int column for given entity_ids */
export function getColumnInt(table: string, field: string, entityIds: number[]): number[]

/** Read text column for given entity_ids */
export function getColumnText(table: string, field: string, entityIds: number[]): string[]

/** Get parent entity_id */
export function parent(table: string, entityId: number): number | null

/** Get all child entity_ids */
export function children(table: string, parentId: number): number[]

/** Lookup string → value_id in vocabulary */
export function vocabLookup(value: string): number | null

/** Flush + rebuild cylinders */
export function flush(): void

/** Get entity count for table */
export function count(table: string): number
