export interface LoginRequest { email: string; password: string; }
export interface RegisterRequest { email: string; password: string; fullName: string; }
export interface AuthResponse { token: string; refreshToken: string; expiration: string; user: UserDto; }
export interface UserDto { id: string; email: string; fullName: string; role?: string; profile?: UserProfileDto; }
export interface UserProfileDto { phone?: string; address1?: string; address2?: string; city?: string; state?: string; zip?: string; country?: string; latitude?: number; longitude?: number; }
export interface UpdateProfileRequest { fullName?: string; phone?: string; address1?: string; address2?: string; city?: string; state?: string; zip?: string; country?: string; latitude?: number; longitude?: number; }
export interface PagedResult<T> { items: T[]; total: number; page: number; pageSize: number; totalPages: number; }
export interface ClassDto { id: number; name: string; description?: string; isActive: boolean; }
export interface SubjectDto { id: number; name: string; description?: string; isActive: boolean; }
export interface SectionDto { id: number; name: string; capacity: number; isActive: boolean; }
export interface TimelineDto { id: number; name: string; startDate: string; endDate: string; isActive: boolean; }
export interface ClassOfferingDto { id: number; classId: number; className?: string; subjectId: number; subjectName?: string; sectionId: number; sectionName?: string; timelineId: number; timelineName?: string; startDate?: string; endDate?: string; instructorId?: string; instructorName?: string; }
export interface OfferingFilterRequest { page?: number; pageSize?: number; sort?: string; dir?: string; classId?: number; subjectId?: number; sectionId?: number; startDate?: string; endDate?: string; }
export interface ProductDto { id: number; sku: string; name: string; description?: string; unitPrice: number; stockQuantity: number; isActive: boolean; }
export interface CustomerDto { id: number; name: string; email?: string; phone?: string; address?: string; isActive: boolean; }
export interface SupplierDto { id: number; name: string; email?: string; phone?: string; address?: string; isActive: boolean; }
export interface SaleItemDto { id: number; productId: number; productName?: string; quantity: number; unitPrice: number; lineTotal: number; }
export interface SaleDto { id: number; date: string; customerId: number; customerName?: string; total: number; status: string; notes?: string; items: SaleItemDto[]; }
export interface PurchaseItemDto { id: number; productId: number; productName?: string; quantity: number; unitPrice: number; lineTotal: number; }
export interface PurchaseDto { id: number; date: string; supplierId: number; supplierName?: string; total: number; status: string; notes?: string; items: PurchaseItemDto[]; }
export interface PaymentDto { id: number; date: string; amount: number; direction: string; referenceId?: number; referenceType?: string; notes?: string; }
export interface BotQueryRequest { query: string; }
export interface BotCard { title: string; subtitle?: string; data: Record<string, string>; }
export interface BotAction { label: string; action: string; parameters?: Record<string, string>; }
export interface BotReply { text: string; intent: string; confidence: number; cards?: BotCard[]; actions?: BotAction[]; }
