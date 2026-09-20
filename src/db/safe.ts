export async function safeQuery<T>(promise: Promise<T>) {
  try {
    const data = await promise;
    return { data, error: null };
  } catch (error) {
    return { data: null, error };
  }
}

export async function safeQuerySingle<T>(promise: Promise<T[]>) {
  try {
    const data = await promise;
    return { data: data[0] || null, error: null };
  } catch (error) {
    return { data: null, error };
  }
}
