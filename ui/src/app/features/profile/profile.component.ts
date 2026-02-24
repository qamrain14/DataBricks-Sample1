import { Component, signal, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { RouterLink } from '@angular/router';
import { AuthService } from '../../core/services/auth.service';
import { UserDto, UpdateProfileRequest } from '../../core/models';

@Component({
  selector: 'app-profile',
  standalone: true,
  imports: [CommonModule, FormsModule, RouterLink],
  template: `
    <div class="page">
      <header><a routerLink="/" class="back">← Dashboard</a><h1>My Profile</h1></header>
      @if (user()) {
        <div class="profile-card">
          <div class="info-section">
            <h2>{{ user()!.fullName }}</h2>
            <p class="email">{{ user()!.email }}</p>
            <span class="role-badge">{{ user()!.role || 'User' }}</span>
          </div>
          <form class="edit-form" (ngSubmit)="save()">
            @if (message()) { <div [class]="'alert ' + (success()?'success':'error')">{{ message() }}</div> }
            <div class="form-row">
              <div class="field"><label>Full Name</label><input [(ngModel)]="form.fullName" name="fullName" /></div>
              <div class="field"><label>Phone</label><input [(ngModel)]="form.phone" name="phone" /></div>
            </div>
            <div class="form-row">
              <div class="field"><label>Address Line 1</label><input [(ngModel)]="form.address1" name="address1" /></div>
              <div class="field"><label>Address Line 2</label><input [(ngModel)]="form.address2" name="address2" /></div>
            </div>
            <div class="form-row">
              <div class="field"><label>City</label><input [(ngModel)]="form.city" name="city" /></div>
              <div class="field"><label>State</label><input [(ngModel)]="form.state" name="state" /></div>
            </div>
            <div class="form-row">
              <div class="field"><label>Zip</label><input [(ngModel)]="form.zip" name="zip" /></div>
              <div class="field"><label>Country</label><input [(ngModel)]="form.country" name="country" /></div>
            </div>
            <button type="submit" [disabled]="saving()">{{ saving() ? 'Saving...' : 'Save Changes' }}</button>
          </form>
        </div>
      }
    </div>
  `,
  styles: [`
    .page{padding:2rem;max-width:800px;margin:0 auto}header{margin-bottom:1.5rem}.back{color:#667eea;text-decoration:none;display:inline-block;margin-bottom:.5rem}h1{margin:0}
    .profile-card{background:#fff;border-radius:12px;box-shadow:0 2px 8px rgba(0,0,0,.1);padding:2rem}
    .info-section{margin-bottom:2rem;padding-bottom:1.5rem;border-bottom:1px solid #eee}
    .info-section h2{margin:0 0 .25rem}.email{color:#666;margin:0 0 .75rem}
    .role-badge{background:#667eea;color:#fff;padding:.25rem .75rem;border-radius:12px;font-size:.85rem}
    .edit-form{display:flex;flex-direction:column;gap:1rem}
    .form-row{display:grid;grid-template-columns:1fr 1fr;gap:1rem}
    .field{display:flex;flex-direction:column;gap:.25rem}
    .field label{font-weight:500;font-size:.9rem;color:#333}
    .field input{padding:.75rem;border:1px solid #ddd;border-radius:6px;font-size:1rem}
    .field input:focus{outline:none;border-color:#667eea}
    button{padding:.875rem;background:#667eea;color:#fff;border:none;border-radius:6px;font-size:1rem;cursor:pointer;margin-top:.5rem}
    button:disabled{opacity:.6;cursor:not-allowed}
    .alert{padding:.75rem;border-radius:6px;text-align:center;margin-bottom:.5rem}
    .success{background:#d4edda;color:#155724}.error{background:#f8d7da;color:#721c24}
  `]
})
export class ProfileComponent implements OnInit {
  user = signal<UserDto | null>(null);
  saving = signal(false);
  message = signal('');
  success = signal(false);
  form: UpdateProfileRequest = {};

  constructor(private authService: AuthService) {}

  ngOnInit(): void {
    const u = this.authService.currentUser();
    if (u) {
      this.user.set(u);
      this.form = {
        fullName: u.fullName,
        phone: u.profile?.phone || '',
        address1: u.profile?.address1 || '',
        address2: u.profile?.address2 || '',
        city: u.profile?.city || '',
        state: u.profile?.state || '',
        zip: u.profile?.zip || '',
        country: u.profile?.country || ''
      };
    }
  }

  save(): void {
    this.saving.set(true);
    this.message.set('');
    this.authService.updateProfile(this.form).subscribe({
      next: (updated) => {
        this.user.set(updated);
        this.saving.set(false);
        this.success.set(true);
        this.message.set('Profile updated successfully!');
      },
      error: () => {
        this.saving.set(false);
        this.success.set(false);
        this.message.set('Failed to update profile.');
      }
    });
  }
}
