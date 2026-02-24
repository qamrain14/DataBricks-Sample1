import { Component, OnInit, signal, computed } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterLink } from '@angular/router';
import { AuthService } from '../../core/services/auth.service';
import { CommerceService } from '../../core/services/commerce.service';
import { CurriculumService } from '../../core/services/curriculum.service';

@Component({
  selector: 'app-dashboard',
  standalone: true,
  imports: [CommonModule, RouterLink],
  template: `
    <div class="dashboard">
      <header class="dash-header">
        <h1>School Suite Dashboard</h1>
        <div class="user-info">
          <span>{{ userName() }}</span>
          <button class="btn-logout" (click)="logout()">Logout</button>
        </div>
      </header>
      <div class="stats-grid">
        <div class="stat-card"><div class="stat-label">Products</div><div class="stat-value">{{ stats().products }}</div></div>
        <div class="stat-card"><div class="stat-label">Customers</div><div class="stat-value">{{ stats().customers }}</div></div>
        <div class="stat-card"><div class="stat-label">Classes</div><div class="stat-value">{{ stats().classes }}</div></div>
        <div class="stat-card"><div class="stat-label">Subjects</div><div class="stat-value">{{ stats().subjects }}</div></div>
      </div>
      <nav class="nav-grid">
        <a routerLink="/curriculum" class="nav-card"><h3>Curriculum</h3><p>Classes, subjects, sections &amp; timelines</p></a>
        <a routerLink="/commerce" class="nav-card"><h3>Commerce</h3><p>Products, sales, purchases &amp; inventory</p></a>
        <a routerLink="/bot" class="nav-card"><h3>Supervisor Bot</h3><p>AI-powered insights &amp; reports</p></a>
        <a routerLink="/profile" class="nav-card"><h3>Profile</h3><p>Manage your account settings</p></a>
      </nav>
    </div>
  `,
  styles: [`
    .dashboard { padding: 2rem; max-width: 1200px; margin: 0 auto; }
    .dash-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 2rem; padding-bottom: 1rem; border-bottom: 1px solid #eee; }
    .dash-header h1 { margin: 0; font-size: 1.5rem; }
    .user-info { display: flex; align-items: center; gap: 1rem; }
    .btn-logout { padding: 0.5rem 1rem; background: #dc3545; color: white; border: none; border-radius: 4px; cursor: pointer; }
    .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1.5rem; margin-bottom: 2rem; }
    .stat-card { background: white; border-radius: 12px; padding: 1.5rem; box-shadow: 0 2px 8px rgba(0,0,0,0.1); text-align: center; }
    .stat-label { color: #666; font-size: 0.9rem; margin-bottom: 0.5rem; }
    .stat-value { font-size: 2rem; font-weight: 700; color: #333; }
    .nav-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 1.5rem; }
    .nav-card { background: white; border-radius: 12px; padding: 1.5rem; box-shadow: 0 2px 8px rgba(0,0,0,0.1); text-decoration: none; color: inherit; transition: transform 0.2s; }
    .nav-card:hover { transform: translateY(-4px); box-shadow: 0 4px 16px rgba(0,0,0,0.15); }
    .nav-card h3 { margin: 0 0 0.5rem; color: #333; }
    .nav-card p { margin: 0; color: #666; font-size: 0.9rem; }
  `]
})
export class DashboardComponent implements OnInit {
  stats = signal({ products: 0, customers: 0, classes: 0, subjects: 0 });
  userName = computed(() => this.authService.currentUser()?.fullName || 'User');
  constructor(private authService: AuthService, private commerceService: CommerceService, private curriculumService: CurriculumService) {}
  ngOnInit(): void {
    this.commerceService.getProducts(1, 1).subscribe(r => this.stats.update(s => ({ ...s, products: r.total })));
    this.commerceService.getCustomers(1, 1).subscribe(r => this.stats.update(s => ({ ...s, customers: r.total })));
    this.curriculumService.getClasses(1, 1).subscribe(r => this.stats.update(s => ({ ...s, classes: r.total })));
    this.curriculumService.getSubjects(1, 1).subscribe(r => this.stats.update(s => ({ ...s, subjects: r.total })));
  }
  logout(): void { this.authService.logout(); }
}
