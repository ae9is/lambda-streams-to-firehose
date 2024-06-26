/**
 * Return an object with only specified keys in it
 */
export function filter(object: any, filterKeys: string[]) {
  return Object.keys(object)
    .filter((key) => filterKeys.includes(key))
    .reduce((filtered: any, key: string) => {
      filtered[key] = object[key]
      return filtered
    }, {})
}

// Use as array.filter(notEmpty)
// ref: https://stackoverflow.com/a/46700791
export function notEmpty<TValue>(value: TValue | null | undefined): value is TValue {
  if (value === null || value === undefined) return false
  const testDummy: TValue = value
  return true
}

export function stringify(value: unknown) {
  let stringified
  try {
    stringified = JSON.stringify(value)
  } catch (e) {
    console.error(`Error stringifying ${value}`)
  }
  return stringified
}
