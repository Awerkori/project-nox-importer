export async function safeQuery(promise) {
    try {
        const data = await promise;
        return { data, error: null };
    }
    catch (error) {
        return { data: null, error };
    }
}
export async function safeQuerySingle(promise) {
    try {
        const data = await promise;
        return { data: data[0] || null, error: null };
    }
    catch (error) {
        return { data: null, error };
    }
}
